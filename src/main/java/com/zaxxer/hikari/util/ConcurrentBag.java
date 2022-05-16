/*
 * Copyright (C) 2013, 2014 Brett Wooldridge
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
package com.zaxxer.hikari.util;

import com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static com.zaxxer.hikari.util.ClockSource.currentTime;
import static com.zaxxer.hikari.util.ClockSource.elapsedNanos;
import static com.zaxxer.hikari.util.ConcurrentBag.IConcurrentBagEntry.*;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

/**
 * This is a specialized concurrent bag that achieves superior performance
 * to LinkedBlockingQueue and LinkedTransferQueue for the purposes of a
 * connection pool.  It uses ThreadLocal storage when possible to avoid
 * locks, but resorts to scanning a common collection if there are no
 * available items in the ThreadLocal list.  Not-in-use items in the
 * ThreadLocal lists can be "stolen" when the borrowing thread has none
 * of its own.  It is a "lock-less" implementation using a specialized
 * AbstractQueuedLongSynchronizer to manage cross-thread signaling.
 *
 * Note that items that are "borrowed" from the bag are not actually
 * removed from any collection, so garbage collection will not occur
 * even if the reference is abandoned.  Thus care must be taken to
 * "requite" borrowed objects otherwise a memory leak will result.  Only
 * the "remove" method can completely remove an object from the bag.
 *
 * @author Brett Wooldridge // ConcurrentBag 中全部的资源均只能通过 add 方法进行添加，只能通过 remove 方法进行移出
 *
 * @param <T> the templated type to store in the bag
 */
public class ConcurrentBag<T extends IConcurrentBagEntry> implements AutoCloseable // 一个专门的并发包裹，在连接池（多线程数据交互）的实现上具有比LinkedBlockingQueue和LinkedTransferQueue更优越的性能。
{
   private static final Logger LOGGER = LoggerFactory.getLogger(ConcurrentBag.class);

   private final CopyOnWriteArrayList<T> sharedList; // 负责存放 ConcurrentBag 中全部用于出借的资源
   private final boolean weakThreadLocals; // 来判断是否使用弱引用

   private final ThreadLocal<List<Object>> threadList; // 用于加速线程本地化资源访问
   private final IBagStateListener listener;
   private final AtomicInteger waiters;
   private volatile boolean closed;

   private final SynchronousQueue<T> handoffQueue; // 用于存在资源等待线程时的第一手资源交接

   public interface IConcurrentBagEntry
   {
      int STATE_NOT_IN_USE = 0; // 未使用，即闲置中
      int STATE_IN_USE = 1; // 使用中
      int STATE_REMOVED = -1; // 被废弃
      int STATE_RESERVED = -2; // 保留态，中间状态，用于尝试驱逐连接对象时

      boolean compareAndSet(int expectState, int newState);
      void setState(int newState);
      int getState();
   }

   public interface IBagStateListener
   {
      void addBagItem(int waiting);
   }

   /**
    * Construct a ConcurrentBag with the specified listener.
    *
    * @param listener the IBagStateListener to attach to this bag
    */
   public ConcurrentBag(final IBagStateListener listener)
   {
      this.listener = listener;
      this.weakThreadLocals = useWeakThreadLocals();
      // SynchronousQueue 无存储空间的阻塞队列
      this.handoffQueue = new SynchronousQueue<>(true); // 公平模式
      this.waiters = new AtomicInteger();
      this.sharedList = new CopyOnWriteArrayList<>();
      if (weakThreadLocals) {
         this.threadList = ThreadLocal.withInitial(() -> new ArrayList<>(16));
      }
      else {
         this.threadList = ThreadLocal.withInitial(() -> new FastList<>(IConcurrentBagEntry.class, 16));
      }
   }

   /**
    * The method will borrow a BagEntry from the bag, blocking for the
    * specified timeout if none are available.
    * 进行数据资源借用，只提供对象引用，不移除对象，因此使用时通过 borrow 取出的对象必须通过 requite 方法进行放回，否则容易导致内存泄露
    * @param timeout how long to wait before giving up, in units of unit
    * @param timeUnit a <code>TimeUnit</code> determining how to interpret the timeout parameter
    * @return a borrowed instance from the bag or null if a timeout occurs
    * @throws InterruptedException if interrupted while waiting
    */
   public T borrow(long timeout, final TimeUnit timeUnit) throws InterruptedException
   {
      // Try the thread-local list first 优先查看 ThreadLocal 中有没有可用的本地化的资源
      final var list = threadList.get();
      for (int i = list.size() - 1; i >= 0; i--) {
         final var entry = list.remove(i);
         @SuppressWarnings("unchecked")
         final T bagEntry = weakThreadLocals ? ((WeakReference<T>) entry).get() : (T) entry;
         if (bagEntry != null && bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) { // 将状态由 STATE_NOT_IN_USE 改为 STATE_IN_USE
            return bagEntry;
         }
      }

      // Otherwise, scan the shared list ... then poll the handoff queue
      final int waiting = waiters.incrementAndGet();
      try {
         for (T bagEntry : sharedList) { // 当 ThreadLocal 中无可用本地化资源时，遍历全部资源，查看是否存在可用资源， 因此被一个线程本地化的资源也可能被另一个线程“抢走”
            if (bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) { // 抢到了，将状态由 STATE_NOT_IN_USE 改为 STATE_IN_USE
               // If we may have stolen another waiter's connection, request another bag add.
               if (waiting > 1) {
                  listener.addBagItem(waiting - 1); // 因为可能“抢走”了其他线程的资源，因此提醒包裹进行资源添加 waiting - 1 个资源
               }
               return bagEntry;
            }
         }

         listener.addBagItem(waiting);
         // 如果还没获取到，会堵塞等待空闲连接
         timeout = timeUnit.toNanos(timeout);
         do { // 这里会出现三种情况，1.超时，返回null 2.获取到资源，但状态为正在使用，继续循环 3.获取到资源，元素状态为未使用，修改为已使用并返回
            final var start = currentTime(); // 一定时间内轮询 handoff 队列
            final T bagEntry = handoffQueue.poll(timeout, NANOSECONDS); // 当现有全部资源全部在使用中，等待一个被释放的资源或者一个新资源
            if (bagEntry == null || bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_IN_USE)) {
               return bagEntry;
            }

            timeout -= elapsedNanos(start);
         } while (timeout > 10_000);

         return null; // 超时了还是没有获取到，返回null
      }
      finally {
         waiters.decrementAndGet();
      }
   }

   /**
    * This method will return a borrowed object to the bag.  Objects
    * that are borrowed from the bag but never "requited" will result
    * in a memory leak.
    *
    * @param bagEntry the value to return to the bag
    * @throws NullPointerException if value is null
    * @throws IllegalStateException if the bagEntry was not borrowed from the bag
    */
   public void requite(final T bagEntry) // 进行资源回收
   {
      bagEntry.setState(STATE_NOT_IN_USE); // 将状态转为 未在使用 STATE_NOT_IN_USE
      // 判断是否存在等待线程，若存在，则直接转手资源
      for (var i = 0; waiters.get() > 0; i++) {
         if (bagEntry.getState() != STATE_NOT_IN_USE || handoffQueue.offer(bagEntry)) {
            return;
         }
         else if ((i & 0xff) == 0xff) {
            parkNanos(MICROSECONDS.toNanos(10));
         }
         else {
            Thread.yield();
         }
      }
      // 否则，进行资源本地化
      final var threadLocalList = threadList.get();
      if (threadLocalList.size() < 50) {
         threadLocalList.add(weakThreadLocals ? new WeakReference<>(bagEntry) : bagEntry);
      }
   }

   /**
    * Add a new object to the bag for others to borrow.
    *
    * @param bagEntry an object to add to the bag
    */
   public void add(final T bagEntry) // 全部的资源均只能通过add方法进行添加
   {
      if (closed) {
         LOGGER.info("ConcurrentBag has been closed, ignoring add()");
         throw new IllegalStateException("ConcurrentBag has been closed, ignoring add()");
      }

      sharedList.add(bagEntry);  // 新添加的资源优先放入 CopyOnWriteArrayList
      // 【当有等待资源的线程时】，将资源交到某个等待线程后才返回（SynchronousQueue）
      // spin until a thread takes it or none are waiting 【自旋】( 等待者 > 0 && 状态是 STATE_NOT_IN_USE && 往交接队列塞的时候没塞成功)直到一个线程取走这个连接或没有人在等待
      while (waiters.get() > 0 && bagEntry.getState() == STATE_NOT_IN_USE && !handoffQueue.offer(bagEntry)) { // !handoffQueue.offer(bagEntry) 往交接队列塞的时候没塞成功
         Thread.yield();
      }
   }

   /**
    * Remove a value from the bag.  This method should only be called
    * with objects obtained by <code>borrow(long, TimeUnit)</code> or <code>reserve(T)</code>
    *
    * @param bagEntry the value to remove
    * @return true if the entry was removed, false otherwise
    * @throws IllegalStateException if an attempt is made to remove an object
    *         from the bag that was not borrowed or reserved first
    */
   public boolean remove(final T bagEntry) // 资源只能通过remove方法进行移出
   {     // 如果资源正在使用且无法进行状态切换，则返回失败
      if (!bagEntry.compareAndSet(STATE_IN_USE, STATE_REMOVED) && !bagEntry.compareAndSet(STATE_RESERVED, STATE_REMOVED) && !closed) {
         LOGGER.warn("Attempt to remove an object from the bag that was not borrowed or reserved: {}", bagEntry);
         return false;
      }

      final boolean removed = sharedList.remove(bagEntry);  // 从 CopyOnWriteArrayList 中移出
      if (!removed && !closed) {
         LOGGER.warn("Attempt to remove an object from the bag that does not exist: {}", bagEntry);
      }

      threadList.get().remove(bagEntry);

      return removed;
   }

   /**
    * Close the bag to further adds.
    */
   @Override
   public void close()
   {
      closed = true;
   }

   /**
    * This method provides a "snapshot" in time of the BagEntry
    * items in the bag in the specified state.  It does not "lock"
    * or reserve items in any way.  Call <code>reserve(T)</code>
    * on items in list before performing any action on them.
    *
    * @param state one of the {@link IConcurrentBagEntry} states
    * @return a possibly empty list of objects having the state specified
    */
   public List<T> values(final int state)
   {
      final var list = sharedList.stream().filter(e -> e.getState() == state).collect(Collectors.toList());
      Collections.reverse(list);
      return list;
   }

   /**
    * This method provides a "snapshot" in time of the bag items.  It
    * does not "lock" or reserve items in any way.  Call <code>reserve(T)</code>
    * on items in the list, or understand the concurrency implications of
    * modifying items, before performing any action on them.
    *
    * @return a possibly empty list of (all) bag items
    */
   @SuppressWarnings("unchecked")
   public List<T> values()
   {
      return (List<T>) sharedList.clone();
   }

   /**
    * The method is used to make an item in the bag "unavailable" for
    * borrowing.  It is primarily used when wanting to operate on items
    * returned by the <code>values(int)</code> method.  Items that are
    * reserved can be removed from the bag via <code>remove(T)</code>
    * without the need to unreserve them.  Items that are not removed
    * from the bag can be make available for borrowing again by calling
    * the <code>unreserve(T)</code> method.
    *
    * @param bagEntry the item to reserve
    * @return true if the item was able to be reserved, false otherwise
    */
   public boolean reserve(final T bagEntry) // 将连接标记为 STATE_RESERVED 保留状态
   {  // 标记成功，返回 true
      return bagEntry.compareAndSet(STATE_NOT_IN_USE, STATE_RESERVED); // 设置为 STATE_RESERVED 保留状态
   }

   /**
    * This method is used to make an item reserved via <code>reserve(T)</code>
    * available again for borrowing.
    *
    * @param bagEntry the item to unreserve
    */
   @SuppressWarnings("SpellCheckingInspection")
   public void unreserve(final T bagEntry)
   {
      if (bagEntry.compareAndSet(STATE_RESERVED, STATE_NOT_IN_USE)) {
         // spin until a thread takes it or none are waiting
         while (waiters.get() > 0 && !handoffQueue.offer(bagEntry)) {
            Thread.yield();
         }
      }
      else {
         LOGGER.warn("Attempt to relinquish an object to the bag that was not reserved: {}", bagEntry);
      }
   }

   /**
    * Get the number of threads pending (waiting) for an item from the
    * bag to become available.
    *
    * @return the number of threads waiting for items from the bag
    */
   public int getWaitingThreadCount()
   {
      return waiters.get();
   }

   /**
    * Get a count of the number of items in the specified state at the time of this call.
    *
    * @param state the state of the items to count
    * @return a count of how many items in the bag are in the specified state
    */
   public int getCount(final int state)
   {
      var count = 0;
      for (var e : sharedList) {
         if (e.getState() == state) {
            count++;
         }
      }
      return count;
   }

   public int[] getStateCounts()
   {
      final var states = new int[6];
      for (var e : sharedList) {
         ++states[e.getState()];
      }
      states[4] = sharedList.size();
      states[5] = waiters.get();

      return states;
   }

   /**
    * Get the total number of items in the bag.
    *
    * @return the number of items in the bag
    */
   public int size()
   {
      return sharedList.size();
   }

   public void dumpState()
   {
      sharedList.forEach(entry -> LOGGER.info(entry.toString()));
   }

   /**
    * Determine whether to use WeakReferences based on whether there is a
    * custom ClassLoader implementation sitting between this class and the
    * System ClassLoader.
    *
    * @return true if we should use WeakReferences in our ThreadLocals, false otherwise
    */
   private boolean useWeakThreadLocals() // 初始化 weakThreadLocals 变量
   {
      try { // 人工指定是否使用弱引用，但是官方不推荐进行自主设置
         if (System.getProperty("com.zaxxer.hikari.useWeakReferences") != null) {   // undocumented manual override of WeakReference behavior
            return Boolean.getBoolean("com.zaxxer.hikari.useWeakReferences");
         }

         return getClass().getClassLoader() != ClassLoader.getSystemClassLoader(); // 默认通过判断初始化的 ClassLoader 是否是 系统ClassLoader 来确定
      }
      catch (SecurityException se) {
         return true;
      }
   }
}
