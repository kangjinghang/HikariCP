/*
 * Copyright (C) 2013,2014 Brett Wooldridge
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zaxxer.hikari.pool;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariPoolMXBean;
import com.zaxxer.hikari.metrics.MetricsTrackerFactory;
import com.zaxxer.hikari.metrics.PoolStats;
import com.zaxxer.hikari.metrics.dropwizard.CodahaleHealthChecker;
import com.zaxxer.hikari.metrics.dropwizard.CodahaleMetricsTrackerFactory;
import com.zaxxer.hikari.metrics.micrometer.MicrometerMetricsTrackerFactory;
import com.zaxxer.hikari.util.ConcurrentBag;
import com.zaxxer.hikari.util.ConcurrentBag.IBagStateListener;
import com.zaxxer.hikari.util.SuspendResumeLock;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.zaxxer.hikari.util.ClockSource.*;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_IN_USE;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.STATE_NOT_IN_USE;
import static com.zaxxer.hikari.util.UtilityElf.*;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This is the primary connection pool class that provides the basic
 * pooling behavior for HikariCP.
 *
 * @author Brett Wooldridge
 */
public final class HikariPool extends PoolBase implements HikariPoolMXBean, IBagStateListener
{
   private final Logger logger = LoggerFactory.getLogger(HikariPool.class);

   public static final int POOL_NORMAL = 0;
   public static final int POOL_SUSPENDED = 1;
   public static final int POOL_SHUTDOWN = 2;

   public volatile int poolState;

   private final long aliveBypassWindowMs = Long.getLong("com.zaxxer.hikari.aliveBypassWindowMs", MILLISECONDS.toMillis(500));
   private final long housekeepingPeriodMs = Long.getLong("com.zaxxer.hikari.housekeeping.periodMs", SECONDS.toMillis(30));

   private static final String EVICTED_CONNECTION_MESSAGE = "(connection was evicted)";
   private static final String DEAD_CONNECTION_MESSAGE = "(connection is dead)";

   private final PoolEntryCreator poolEntryCreator = new PoolEntryCreator(); // 主要会被 addConnectionExecutor 用到
   private final PoolEntryCreator postFillPoolEntryCreator = new PoolEntryCreator("After adding "); //
   private final AtomicInteger addConnectionQueueDepth = new AtomicInteger();
   private final ThreadPoolExecutor addConnectionExecutor; // 执行创建连接任务的线程池。只开启一个线程执行任务
   private final ThreadPoolExecutor closeConnectionExecutor; // 执行关闭原生连接任务的线程池。只开启一个线程执行任务

   private final ConcurrentBag<PoolEntry> connectionBag;

   private final ProxyLeakTaskFactory leakTaskFactory;
   private final SuspendResumeLock suspendResumeLock;

   private final ScheduledExecutorService houseKeepingExecutorService; // 用于执行检查 idleTimeout、leakDetectionThreshold、keepaliveTime、maxLifetime 等任务的线程池
   private ScheduledFuture<?> houseKeeperTask;

   /**
    * Construct a HikariPool with the specified configuration.
    *
    * @param config a HikariConfig instance
    */
   public HikariPool(final HikariConfig config)
   {
      super(config);
      // isAllowPoolSuspension 默认值是 false 的，构造函数直接会创建虚的 SuspendResumeLock.FAUX_LOCK，只有 isAllowPoolSuspension 为 true 时，才会真正创建 SuspendResumeLock
      this.connectionBag = new ConcurrentBag<>(this);
      this.suspendResumeLock = config.isAllowPoolSuspension() ? new SuspendResumeLock() : SuspendResumeLock.FAUX_LOCK;
      // 初始化 houseKeepingExecutorService
      this.houseKeepingExecutorService = initializeHouseKeepingExecutorService();

      checkFailFast();

      if (config.getMetricsTrackerFactory() != null) {
         setMetricsTrackerFactory(config.getMetricsTrackerFactory());
      }
      else {
         setMetricRegistry(config.getMetricRegistry());
      }

      setHealthCheckRegistry(config.getHealthCheckRegistry());

      handleMBeans(this, true);

      ThreadFactory threadFactory = config.getThreadFactory();

      final int maxPoolSize = config.getMaximumPoolSize();
      LinkedBlockingQueue<Runnable> addConnectionQueue = new LinkedBlockingQueue<>(16);
      this.addConnectionExecutor = createThreadPoolExecutor(addConnectionQueue, poolName + " connection adder", threadFactory, new CustomDiscardPolicy());
      this.closeConnectionExecutor = createThreadPoolExecutor(maxPoolSize, poolName + " connection closer", threadFactory, new ThreadPoolExecutor.CallerRunsPolicy());
      // 初始化 leakTaskFactory
      this.leakTaskFactory = new ProxyLeakTaskFactory(config.getLeakDetectionThreshold(), houseKeepingExecutorService);
      // 默认的是初始化后100毫秒执行，之后每执行完一次之后隔HOUSEKEEPING_PERIOD_MS(30秒)时间执行
      this.houseKeeperTask = houseKeepingExecutorService.scheduleWithFixedDelay(new HouseKeeper(), 100L, housekeepingPeriodMs, MILLISECONDS);

      if (Boolean.getBoolean("com.zaxxer.hikari.blockUntilFilled") && config.getInitializationFailTimeout() > 1) {
         addConnectionExecutor.setMaximumPoolSize(Math.min(16, Runtime.getRuntime().availableProcessors()));
         addConnectionExecutor.setCorePoolSize(Math.min(16, Runtime.getRuntime().availableProcessors()));

         final long startTime = currentTime();
         while (elapsedMillis(startTime) < config.getInitializationFailTimeout() && getTotalConnections() < config.getMinimumIdle()) {
            quietlySleep(MILLISECONDS.toMillis(100));
         }

         addConnectionExecutor.setCorePoolSize(1);
         addConnectionExecutor.setMaximumPoolSize(1);
      }
   }

   /**
    * Get a connection from the pool, or timeout after connectionTimeout milliseconds.
    *
    * @return a java.sql.Connection instance
    * @throws SQLException thrown if a timeout occurs trying to obtain a connection
    */
   public Connection getConnection() throws SQLException
   {
      return getConnection(connectionTimeout);
   }

   /**
    * Get a connection from the pool, or timeout after the specified number of milliseconds.
    * 如果是有空闲连接的情况，hikari是在connectionTimeout时间内不断循环获取下一个空闲连接进行校验，校验失败继续获取下一个空闲连接，直到超时抛出SQLTransientConnectionException。（hikari在获取一个连接的时候，会在connectionTimeout时间内循环把空闲连接挨个validate一次，最后timeout抛出异常；之后的获取连接操作，则一直阻塞connectionTimeout时间再抛出异常）
    * @param hardTimeout the maximum time to wait for a connection from the pool 就是 connectionTimeout，默认值SECONDS.toMillis(30) = 30000（如果小于250毫秒，则被重置回30秒）
    * @return a java.sql.Connection instance
    * @throws SQLException thrown if a timeout occurs trying to obtain a connection
    */
   public Connection getConnection(final long hardTimeout) throws SQLException
   {
      suspendResumeLock.acquire(); // 进行了 suspendResumeLock.acquire() 操作
      final var startTime = currentTime();

      try {
         var timeout = hardTimeout;
         do {
            var poolEntry = connectionBag.borrow(timeout, MILLISECONDS);
            if (poolEntry == null) { // 走到这，说明已经超时了
               break; // We timed out... break and throw exception
            }

            final var now = currentTime();
            if (poolEntry.isMarkedEvicted() || (elapsedMillis(poolEntry.lastAccessed, now) > aliveBypassWindowMs && isConnectionDead(poolEntry.connection))) { // 拿到一个poolEntry后先判断是否已经被标记为待清理或已经超过了设置的最大存活时间（应用配置的最大存活时间不应超过DBA在DB端配置的最大连接存活时间）
               closeConnection(poolEntry, poolEntry.isMarkedEvicted() ? EVICTED_CONNECTION_MESSAGE : DEAD_CONNECTION_MESSAGE); // 若是直接关闭继续循环调用 borrow
               timeout = hardTimeout - elapsedMillis(startTime); // 更新timeout的值，然后重新循环继续获取连接
            }
            else {
               metricsTracker.recordBorrowStats(poolEntry, startTime); // 记录获取连接的等待时间监控指标
               return poolEntry.createProxyConnection(leakTaskFactory.schedule(poolEntry)); // 创建代理连接的时候关联了 ProxyLeakTask
            }
         } while (timeout > 0L);

         metricsTracker.recordBorrowTimeoutStats(startTime);
         throw createTimeoutException(startTime); // 在 acquire之后如果在没有可用连接的情况下超过此时间，则会抛出 SQLException
      }
      catch (InterruptedException e) {
         Thread.currentThread().interrupt();
         throw new SQLException(poolName + " - Interrupted during connection acquisition", e);
      }
      finally {
         suspendResumeLock.release();  // 进行了 suspendResumeLock.release() 操作
      }
   }

   /**
    * Shutdown the pool, closing all idle connections and aborting or closing
    * active connections.
    *
    * @throws InterruptedException thrown if the thread is interrupted during shutdown
    */
   public synchronized void shutdown() throws InterruptedException
   {
      try {
         poolState = POOL_SHUTDOWN;

         if (addConnectionExecutor == null) { // pool never started
            return;
         }

         logPoolState("Before shutdown ");

         if (houseKeeperTask != null) {
            houseKeeperTask.cancel(false);
            houseKeeperTask = null;
         }

         softEvictConnections();

         addConnectionExecutor.shutdown();
         if (!addConnectionExecutor.awaitTermination(getLoginTimeout(), SECONDS)) {
            logger.warn("Timed-out waiting for add connection executor to shutdown");
         }

         destroyHouseKeepingExecutorService();

         connectionBag.close();

         final var assassinExecutor = createThreadPoolExecutor(config.getMaximumPoolSize(), poolName + " connection assassinator",
                                                                           config.getThreadFactory(), new ThreadPoolExecutor.CallerRunsPolicy());
         try {
            final var start = currentTime();
            do {
               abortActiveConnections(assassinExecutor);
               softEvictConnections();
            } while (getTotalConnections() > 0 && elapsedMillis(start) < SECONDS.toMillis(10));
         }
         finally {
            assassinExecutor.shutdown();
            if (!assassinExecutor.awaitTermination(10L, SECONDS)) {
               logger.warn("Timed-out waiting for connection assassin to shutdown");
            }
         }

         shutdownNetworkTimeoutExecutor();
         closeConnectionExecutor.shutdown();
         if (!closeConnectionExecutor.awaitTermination(10L, SECONDS)) {
            logger.warn("Timed-out waiting for close connection executor to shutdown");
         }
      }
      finally {
         logPoolState("After shutdown ");
         handleMBeans(this, false);
         metricsTracker.close();
      }
   }

   /**
    * Evict a Connection from the pool.
    *
    * @param connection the Connection to evict (actually a {@link ProxyConnection})
    */
   public void evictConnection(Connection connection)
   {
      var proxyConnection = (ProxyConnection) connection;
      proxyConnection.cancelLeakTask();   // 在驱逐连接的时候 cancel 掉泄漏检测的这个 task

      try {
         softEvictConnection(proxyConnection.getPoolEntry(), "(connection evicted by user)", !connection.isClosed() /* owner */);
      }
      catch (SQLException e) {
         // unreachable in HikariCP, but we're still forced to catch it
      }
   }

   /**
    * Set a metrics registry to be used when registering metrics collectors.  The HikariDataSource prevents this
    * method from being called more than once.
    *
    * @param metricRegistry the metrics registry instance to use
    */
   @SuppressWarnings("PackageAccessibility")
   public void setMetricRegistry(Object metricRegistry)
   {
      if (metricRegistry != null && safeIsAssignableFrom(metricRegistry, "com.codahale.metrics.MetricRegistry")) {
         setMetricsTrackerFactory(new CodahaleMetricsTrackerFactory((MetricRegistry) metricRegistry));
      }
      else if (metricRegistry != null && safeIsAssignableFrom(metricRegistry, "io.micrometer.core.instrument.MeterRegistry")) {
         setMetricsTrackerFactory(new MicrometerMetricsTrackerFactory((MeterRegistry) metricRegistry));
      }
      else {
         setMetricsTrackerFactory(null);
      }
   }

   /**
    * Set the MetricsTrackerFactory to be used to create the IMetricsTracker instance used by the pool.
    *
    * @param metricsTrackerFactory an instance of a class that subclasses MetricsTrackerFactory
    */
   public void setMetricsTrackerFactory(MetricsTrackerFactory metricsTrackerFactory)
   {
      if (metricsTrackerFactory != null) {
         this.metricsTracker = new MetricsTrackerDelegate(metricsTrackerFactory.create(config.getPoolName(), getPoolStats()));
      }
      else {
         this.metricsTracker = new NopMetricsTrackerDelegate();
      }
   }

   /**
    * Set the health check registry to be used when registering health checks.  Currently only Codahale health
    * checks are supported.
    *
    * @param healthCheckRegistry the health check registry instance to use
    */
   public void setHealthCheckRegistry(Object healthCheckRegistry)
   {
      if (healthCheckRegistry != null) {
         CodahaleHealthChecker.registerHealthChecks(this, config, (HealthCheckRegistry) healthCheckRegistry);
      }
   }

   // ***********************************************************************
   //                        IBagStateListener callback
   // ***********************************************************************

   /** {@inheritDoc} */
   @Override
   public void addBagItem(final int waiting)
   {
      final int queueDepth = addConnectionQueueDepth.get();
      final int countToAdd = waiting - queueDepth;
      if (countToAdd >= 0) {
         addConnectionQueueDepth.incrementAndGet();
         addConnectionExecutor.submit(poolEntryCreator);
      }
      else {
         logger.debug("{} - Add connection elided, waiting={}, adders pending/running={}", poolName, waiting, queueDepth);
      }
   }

   // ***********************************************************************
   //                        HikariPoolMBean methods
   // ***********************************************************************

   /** {@inheritDoc} */
   @Override
   public int getActiveConnections()
   {
      return connectionBag.getCount(STATE_IN_USE);
   }

   /** {@inheritDoc} */
   @Override
   public int getIdleConnections()
   {
      return connectionBag.getCount(STATE_NOT_IN_USE);
   }

   /** {@inheritDoc} */
   @Override
   public int getTotalConnections()
   {
      return connectionBag.size();
   }

   /** {@inheritDoc} */
   @Override
   public int getThreadsAwaitingConnection()
   {
      return connectionBag.getWaitingThreadCount();
   }

   /** {@inheritDoc} */
   @Override
   public void softEvictConnections() // 从池中逐出当前空闲的连接，并在它们返回池时将活动（正在使用）的连接标记为逐出。
   {
      connectionBag.values().forEach(poolEntry -> softEvictConnection(poolEntry, "(connection evicted)", false /* not owner */)); // owner 默认传 false(not owner)
   }

   /** {@inheritDoc} */
   @Override
   public synchronized void suspendPool()
   {
      if (suspendResumeLock == SuspendResumeLock.FAUX_LOCK) { // 虚的 lock
         throw new IllegalStateException(poolName + " - is not suspendable"); // 如果 suspendResumeLock 是 FAUX_LOCK 的话，就直接抛异常
      }
      else if (poolState != POOL_SUSPENDED) { // 如果当前连接池状态并不是POOL_SUSPENDED（1）
         suspendResumeLock.suspend(); // 调用 SuspendResumeLock 的 suspend 方法，从此信号量获取给定数目10000的许可，在被提供这些许可前一直将线程阻塞
         poolState = POOL_SUSPENDED;
      }
   }

   /** {@inheritDoc} */
   @Override
   public synchronized void resumePool()
   {
      if (poolState == POOL_SUSPENDED) { // 只针对当前是POOL_SUSPENDED 状态的连接池置为 POOL_NORMAL
         poolState = POOL_NORMAL;
         fillPool(false); // 然后fillPool
         suspendResumeLock.resume(); // 实际调用SuspendResumeLock的acquisitionSemaphore.release(MAX_PERMITS)方法释放给定数目MAX_PERMITS 10000的许可，将其返回到信号量
      }
   }

   // ***********************************************************************
   //                           Package methods
   // ***********************************************************************

   /**
    * Log the current pool state at debug level.
    *
    * @param prefix an optional prefix to prepend the log message
    */
   void logPoolState(String... prefix)
   {
      if (logger.isDebugEnabled()) {
         logger.debug("{} - {}stats (total={}, active={}, idle={}, waiting={})",
                      poolName, (prefix.length > 0 ? prefix[0] : ""),
                      getTotalConnections(), getActiveConnections(), getIdleConnections(), getThreadsAwaitingConnection());
      }
   }

   /**
    * Recycle PoolEntry (add back to the pool)
    *
    * @param poolEntry the PoolEntry to recycle
    */
   @Override
   void recycle(final PoolEntry poolEntry)
   {
      metricsTracker.recordConnectionUsage(poolEntry);

      connectionBag.requite(poolEntry);  // poolEntry 通过 borrow 从 bag 中取出，再通过 requite 放回。资源成功回收
   }

   /**
    * Permanently close the real (underlying) connection (eat any exception).
    * 可能是由 evict 标记为true 后，后面有人再次获取连接时进到这里的，也可能是已经超过了设置的最大存活时间要被关闭
    * @param poolEntry poolEntry having the connection to close
    * @param closureReason reason to close
    */
   void closeConnection(final PoolEntry poolEntry, final String closureReason)
   {
      if (connectionBag.remove(poolEntry)) {
         final var connection = poolEntry.close();
         closeConnectionExecutor.execute(() -> {
            quietlyCloseConnection(connection, closureReason);
            if (poolState == POOL_NORMAL) {
               fillPool(false);
            }
         });
      }
   }

   @SuppressWarnings("unused")
   int[] getPoolStateCounts()
   {
      return connectionBag.getStateCounts();
   }


   // ***********************************************************************
   //                           Private methods
   // ***********************************************************************

   /**
    * Creating new poolEntry.  If maxLifetime is configured, create a future End-of-life task with 2.5% variance from
    * the maxLifetime time to ensure there is no massive die-off of Connections in the pool.
    */
   private PoolEntry createPoolEntry()
   {
      try {
         final var poolEntry = newPoolEntry();

         final var maxLifetime = config.getMaxLifetime(); // 默认是 1800000=30分钟
         if (maxLifetime > 0) {
            // variance up to 2.5% of the maxlifetime . 防止同时过期，加一点点随机因素，防止一件事情大量同时发生
            final var variance = maxLifetime > 10_000 ? ThreadLocalRandom.current().nextLong( maxLifetime / 40 ) : 0;
            final var lifetime = maxLifetime - variance;
            poolEntry.setFutureEol(houseKeepingExecutorService.schedule(new MaxLifetimeTask(poolEntry), lifetime, MILLISECONDS));
         }

         final long keepaliveTime = config.getKeepaliveTime();
         if (keepaliveTime > 0) { // 检测活性
            // variance up to 10% of the heartbeat time
            final var variance = ThreadLocalRandom.current().nextLong(keepaliveTime / 10);
            final var heartbeatTime = keepaliveTime - variance;
            poolEntry.setKeepalive(houseKeepingExecutorService.scheduleWithFixedDelay(new KeepaliveTask(poolEntry), heartbeatTime, heartbeatTime, MILLISECONDS));
         }

         return poolEntry;
      }
      catch (ConnectionSetupException e) {
         if (poolState == POOL_NORMAL) { // we check POOL_NORMAL to avoid a flood of messages if shutdown() is running concurrently
            logger.error("{} - Error thrown while acquiring connection from data source", poolName, e.getCause());
            lastConnectionFailure.set(e);
         }
      }
      catch (Exception e) {
         if (poolState == POOL_NORMAL) { // we check POOL_NORMAL to avoid a flood of messages if shutdown() is running concurrently
            logger.debug("{} - Cannot acquire connection from data source", poolName, e);
         }
      }

      return null;
   }

   /**
    * Fill pool up from current idle connections (as they are perceived at the point of execution) to minimumIdle connections.
    */ // 将连接池从当前空闲连接（因为它们在执行点被感知）填充到 minimumIdle 连接（HikariCP尝试在池中维护的最小空闲连接数，如果空闲连接低于此值并且池中的总连接数少于maximumPoolSize，HikariCP将尽最大努力快速高效地添加其他连接）
   private synchronized void fillPool(final boolean isAfterAdd)
   {
      final var queueDepth = addConnectionQueueDepth.get();
      final var countToAdd = connectionBag.getWaitingThreadCount() - queueDepth;
      final var shouldAdd =
            getTotalConnections() < config.getMaximumPoolSize() &&
               (getIdleConnections() < config.getMinimumIdle() || countToAdd > getIdleConnections());

      if (shouldAdd) {
         addConnectionQueueDepth.incrementAndGet();
         addConnectionExecutor.submit(isAfterAdd ? postFillPoolEntryCreator : poolEntryCreator); // 加入异步线程
      }
      else if (isAfterAdd) {
         logger.debug("{} - Fill pool skipped, pool has sufficient level or currently being filled (queueDepth={}).", poolName, queueDepth);
      }
   }

   /**
    * Attempt to abort or close active connections.
    *
    * @param assassinExecutor the ExecutorService to pass to Connection.abort()
    */
   private void abortActiveConnections(final ExecutorService assassinExecutor)
   {
      for (var poolEntry : connectionBag.values(STATE_IN_USE)) {
         Connection connection = poolEntry.close();
         try {
            connection.abort(assassinExecutor);
         }
         catch (Throwable e) {
            quietlyCloseConnection(connection, "(connection aborted during shutdown)");
         }
         finally {
            connectionBag.remove(poolEntry);
         }
      }
   }

   /**
    * If initializationFailFast is configured, check that we have DB connectivity.
    *
    * @throws PoolInitializationException if fails to create or validate connection
    * @see HikariConfig#setInitializationFailTimeout(long)
    */
   private void checkFailFast()
   {
      final var initializationTimeout = config.getInitializationFailTimeout();
      if (initializationTimeout < 0) {
         return;
      }

      final var startTime = currentTime();
      do {
         final var poolEntry = createPoolEntry();
         if (poolEntry != null) {
            if (config.getMinimumIdle() > 0) {
               connectionBag.add(poolEntry);
               logger.info("{} - Added connection {}", poolName, poolEntry.connection);
            }
            else {
               quietlyCloseConnection(poolEntry.close(), "(initialization check complete and minimumIdle is zero)");
            }

            return;
         }

         if (getLastConnectionFailure() instanceof ConnectionSetupException) {
            throwPoolInitializationException(getLastConnectionFailure().getCause());
         }

         quietlySleep(SECONDS.toMillis(1));
      } while (elapsedMillis(startTime) < initializationTimeout);

      if (initializationTimeout > 0) {
         throwPoolInitializationException(getLastConnectionFailure());
      }
   }

   /**
    * Log the Throwable that caused pool initialization to fail, and then throw a PoolInitializationException with
    * that cause attached.
    *
    * @param t the Throwable that caused the pool to fail to initialize (possibly null)
    */
   private void throwPoolInitializationException(Throwable t)
   {
      logger.error("{} - Exception during pool initialization.", poolName, t);
      destroyHouseKeepingExecutorService();
      throw new PoolInitializationException(t);
   }

   /**
    * "Soft" evict a Connection (/PoolEntry) from the pool.  If this method is being called by the user directly
    * through {@link com.zaxxer.hikari.HikariDataSource#evictConnection(Connection)} then {@code owner} is {@code true}.
    *
    * If the caller is the owner, or if the Connection is idle (i.e. can be "reserved" in the {@link ConcurrentBag}),
    * then we can close the connection immediately.  Otherwise, we leave it "marked" for eviction so that it is evicted
    * the next time someone tries to acquire it from the pool.
    * 这是一种 “软” 驱逐池中连接的方法，如果调用方是 owner 身份，或者连接处于空闲状态，可以立即【关闭连接】。否则，我们将其【“标记”为驱逐】，以便下次有人试图从池中获取它时将其逐出
    * @param poolEntry the PoolEntry (/Connection) to "soft" evict from the pool
    * @param reason the reason that the connection is being evicted
    * @param owner true if the caller is the owner of the connection, false otherwise
    * @return true if the connection was evicted (closed), false if it was merely marked for eviction
    */
   private boolean softEvictConnection(final PoolEntry poolEntry, final String reason, final boolean owner)
   {
      poolEntry.markEvicted();
      if (owner || connectionBag.reserve(poolEntry)) { // 调用方是 owner 身份，或者当 owner 是 false 的时候从 connectionBag 中成功的将连接状态标记为 STATE_RESERVED
         closeConnection(poolEntry, reason); // 可以立即【关闭连接】，返回true
         return true;
      }
      // 否则，只是将 evict 标记为true，返回 false
      return false;
   }

   /**
    * Create/initialize the Housekeeping service {@link ScheduledExecutorService}.  If the user specified an Executor
    * to be used in the {@link HikariConfig}, then we use that.  If no Executor was specified (typical), then create
    * an Executor and configure it.
    *
    * @return either the user specified {@link ScheduledExecutorService}, or the one we created
    */
   private ScheduledExecutorService initializeHouseKeepingExecutorService()
   {
      if (config.getScheduledExecutor() == null) {
         final var threadFactory = Optional.ofNullable(config.getThreadFactory()).orElseGet(() -> new DefaultThreadFactory(poolName + " housekeeper", true));
         final var executor = new ScheduledThreadPoolExecutor(1, threadFactory, new ThreadPoolExecutor.DiscardPolicy());
         executor.setExecuteExistingDelayedTasksAfterShutdownPolicy(false); // 设置为 false，执行shutdown()方法之后，待处理的任务将不会被执行
         executor.setRemoveOnCancelPolicy(true); // 取消任务后从阻塞队列中移除任务，为了防止队列中积压大量已被取消的任务。
         return executor;
      }
      else {
         return config.getScheduledExecutor();
      }
   }

   /**
    * Destroy (/shutdown) the Housekeeping service Executor, if it was the one that we created.
    */
   private void destroyHouseKeepingExecutorService()
   {
      if (config.getScheduledExecutor() == null) {
         houseKeepingExecutorService.shutdownNow();
      }
   }

   /**
    * Create a PoolStats instance that will be used by metrics tracking, with a pollable resolution of 1 second.
    *
    * @return a PoolStats instance
    */
   private PoolStats getPoolStats()
   {
      return new PoolStats(SECONDS.toMillis(1)) {
         @Override
         protected void update() {
            this.pendingThreads = HikariPool.this.getThreadsAwaitingConnection();
            this.idleConnections = HikariPool.this.getIdleConnections();
            this.totalConnections = HikariPool.this.getTotalConnections();
            this.activeConnections = HikariPool.this.getActiveConnections();
            this.maxConnections = config.getMaximumPoolSize();
            this.minConnections = config.getMinimumIdle();
         }
      };
   }

   /**
    * Create a timeout exception (specifically, {@link SQLTransientConnectionException}) to be thrown, because a
    * timeout occurred when trying to acquire a Connection from the pool.  If there was an underlying cause for the
    * timeout, e.g. a SQLException thrown by the driver while trying to create a new Connection, then use the
    * SQL State from that exception as our own and additionally set that exception as the "next" SQLException inside
    * of our exception.
    *
    * As a side-effect, log the timeout failure at DEBUG, and record the timeout failure in the metrics tracker.
    *
    * @param startTime the start time (timestamp) of the acquisition attempt
    * @return a SQLException to be thrown from {@link #getConnection()}
    */
   private SQLException createTimeoutException(long startTime)
   {
      logPoolState("Timeout failure ");
      metricsTracker.recordConnectionTimeout();

      String sqlState = null;
      final var originalException = getLastConnectionFailure();
      if (originalException instanceof SQLException) {
         sqlState = ((SQLException) originalException).getSQLState();
      }
      final var connectionException = new SQLTransientConnectionException(poolName + " - Connection is not available, request timed out after " + elapsedMillis(startTime) + "ms.", sqlState, originalException);
      if (originalException instanceof SQLException) {
         connectionException.setNextException((SQLException) originalException);
      }

      return connectionException;
   }


   // ***********************************************************************
   //                      Non-anonymous Inner-classes
   // ***********************************************************************

   /**
    * Creating and adding poolEntries (connections) to the pool.
    */
   private final class PoolEntryCreator implements Callable<Boolean> // 异步线程
   {
      private final String loggingPrefix;

      PoolEntryCreator()
      {
         this(null);
      }

      PoolEntryCreator(String loggingPrefix)
      {
         this.loggingPrefix = loggingPrefix;
      }

      @Override
      public Boolean call()
      {
         var backoffMs = 10L;
         var added = false;
         try {
            while (shouldContinueCreating()) {
               final var poolEntry = createPoolEntry();
               if (poolEntry != null) {
                  added = true;
                  backoffMs = 10L;
                  connectionBag.add(poolEntry);
                  logger.debug("{} - Added connection {}", poolName, poolEntry.connection);
               } else {  // failed to get connection from db, sleep and retry
                  backoffMs = Math.min(SECONDS.toMillis(5), backoffMs * 2);
                  if (loggingPrefix != null)
                     logger.debug("{} - Connection add failed, sleeping with backoff: {}ms", poolName, backoffMs);
               }

               quietlySleep(backoffMs);
            }
         }
         finally {
            addConnectionQueueDepth.decrementAndGet();
            if (added && loggingPrefix != null) logPoolState(loggingPrefix);
         }

         // Pool is suspended, shutdown, or at max size
         return Boolean.FALSE;
      }

      /**
       * We only create connections if we need another idle connection or have threads still waiting
       * for a new connection.  Otherwise we bail out of the request to create.
       * 只有当我们需要另一个空闲连接或有线程仍在等待新连接时，我们才会创建连接。否则，我们将退出创建请求。
       * @return true if we should create a connection, false if the need has disappeared
       */
      private synchronized boolean shouldContinueCreating() { // 连接池状态是 POOL_NORMAL && 总连接数 < 设置的最大连接数 && (空闲连接数 < 设置的最小连接数 || 等待连接的线程数 > 空闲连接数)
         return poolState == POOL_NORMAL && getTotalConnections() < config.getMaximumPoolSize() &&
            (getIdleConnections() < config.getMinimumIdle() || connectionBag.getWaitingThreadCount() > getIdleConnections());
      }
   }

   /**
    * The house keeping task to retire and maintain minimum idle connections.
    */
   private final class HouseKeeper implements Runnable // 该线程尝试在池中维护的最小空闲连接数，并不断刷新的通过MBean调整的connectionTimeout和validationTimeout等值。
   {
      private volatile long previous = plusMillis(currentTime(), -housekeepingPeriodMs);
      @SuppressWarnings("AtomicFieldUpdaterNotStaticFinal")
      private final AtomicReferenceFieldUpdater<PoolBase, String> catalogUpdater = AtomicReferenceFieldUpdater.newUpdater(PoolBase.class, String.class, "catalog");

      @Override
      public void run()
      {
         try {
            // refresh values in case they changed via MBean 刷新的通过MBean调整的connectionTimeout和validationTimeout等值
            connectionTimeout = config.getConnectionTimeout();
            validationTimeout = config.getValidationTimeout();
            leakTaskFactory.updateLeakDetectionThreshold(config.getLeakDetectionThreshold()); // 管理 leakDetectionThreshold 值

            if (config.getCatalog() != null && !config.getCatalog().equals(catalog)) {
               catalogUpdater.set(HikariPool.this, config.getCatalog());
            }

            final var idleTimeout = config.getIdleTimeout();
            final var now = currentTime();

            // Detect retrograde time, allowing +128ms as per NTP spec. 检测时钟回拨，如果时钟在规定范围外(+128ms)回拨了，就驱除连接，并重置时间
            if (plusMillis(now, 128) < plusMillis(previous, housekeepingPeriodMs)) {
               logger.warn("{} - Retrograde clock change detected (housekeeper delta={}), soft-evicting connections from pool.",
                           poolName, elapsedDisplayString(previous, now));
               previous = now;
               softEvictConnections(); // 驱除连接
               return;
            }
            else if (now > plusMillis(previous, (3 * housekeepingPeriodMs) / 2)) {
               // No point evicting for forward clock motion, this merely accelerates connection retirement anyway
               logger.warn("{} - Thread starvation or clock leap detected (housekeeper delta={}).", poolName, elapsedDisplayString(previous, now));
            }

            previous = now;

            var afterPrefix = "Pool ";
            if (idleTimeout > 0L && config.getMinimumIdle() < config.getMaximumPoolSize()) { // idleTimeout >0 且配置的 minimumIdle < maximumPoolSize 时才开始处理超时的空闲连接
               logPoolState("Before cleanup ");
               afterPrefix = "After cleanup  ";

               final var notInUse = connectionBag.values(STATE_NOT_IN_USE); // 取出状态是STATE_NOT_IN_USE的连接数
               var toRemove = notInUse.size() - config.getMinimumIdle();
               for (PoolEntry entry : notInUse) {
                  if (toRemove > 0 && elapsedMillis(entry.lastAccessed, now) > idleTimeout && connectionBag.reserve(entry)) { // 根据idleTimeout的值，移除掉空闲超时的连接
                     closeConnection(entry, "(connection has passed idleTimeout)"); // 若移除成功则关闭该连接
                     toRemove--;
                  }
               }
            }

            logPoolState(afterPrefix);
            // 在空闲连接移除之后，再调用 fillPool，尝试补充空间连接数到 minimumIdle 值
            fillPool(true); // Try to maintain minimum connections
         }
         catch (Exception e) {
            logger.error("Unexpected exception in housekeeping task", e);
         }
      }
   }

   private class CustomDiscardPolicy implements RejectedExecutionHandler
   {
      @Override
      public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
         addConnectionQueueDepth.decrementAndGet();
      }
   }
   // 延时任务，在连接存活将要到达 maxLifetime 之前触发 evict，用来防止出现大面积的 connection 因 maxLifetime 同一时刻失效
   private final class MaxLifetimeTask implements Runnable
   {
      private final PoolEntry poolEntry;

      MaxLifetimeTask(final PoolEntry poolEntry)
      {
         this.poolEntry = poolEntry;
      }

      public void run()
      {  // 在连接存活将要到达 maxLifetime 之前触发 evict，将连接"软"驱逐出连接池，用来防止出现大面积的 connection 因 maxLifetime 同一时刻失效
         if (softEvictConnection(poolEntry, "(connection has passed maxLifetime)", false /* not owner */)) {
            addBagItem(connectionBag.getWaitingThreadCount());
         }
      }
   }

   private final class KeepaliveTask implements Runnable
   {
      private final PoolEntry poolEntry;

      KeepaliveTask(final PoolEntry poolEntry)
      {
         this.poolEntry = poolEntry;
      }

      public void run()
      {
         if (connectionBag.reserve(poolEntry)) {
            if (isConnectionDead(poolEntry.connection)) {
               softEvictConnection(poolEntry, DEAD_CONNECTION_MESSAGE, true);
               addBagItem(connectionBag.getWaitingThreadCount());
            }
            else {
               connectionBag.unreserve(poolEntry);
               logger.debug("{} - keepalive: connection {} is alive", poolName, poolEntry.connection);
            }
         }
      }
   }

   public static class PoolInitializationException extends RuntimeException
   {
      private static final long serialVersionUID = 929872118275916520L;

      /**
       * Construct an exception, possibly wrapping the provided Throwable as the cause.
       * @param t the Throwable to wrap
       */
      public PoolInitializationException(Throwable t)
      {
         super("Failed to initialize pool: " + t.getMessage(), t);
      }
   }
}
