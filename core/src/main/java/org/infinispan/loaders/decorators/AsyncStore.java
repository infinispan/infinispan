/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.decorators;

import net.jcip.annotations.GuardedBy;
import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.CacheStore;
import org.infinispan.loaders.modifications.Clear;
import org.infinispan.loaders.modifications.Modification;
import org.infinispan.loaders.modifications.ModificationsList;
import org.infinispan.loaders.modifications.Remove;
import org.infinispan.loaders.modifications.Store;
import org.infinispan.marshall.StreamingMarshaller;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.TransactionFactory;
import org.infinispan.util.CollectionFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * The AsyncStore is a delegating CacheStore that buffers changes and writes them asynchronously to
 * the underlying CacheStore.
 * <p/>
 * Read operations are done synchronously, taking into account the current state of buffered changes.
 * <p/>
 * There is no provision for exception handling for problems encountered with the underlying store
 * during a write operation, and the exception is just logged.
 * <p/>
 * When configuring the loader, use the following element:
 * <p/>
 * <code> &lt;async enabled="true" /&gt; </code>
 * <p/>
 * to define whether cache loader operations are to be asynchronous. If not specified, a cache loader operation is
 * assumed synchronous and this decorator is not applied.
 * <p/>
 * Write operations affecting same key are now coalesced so that only the final state is actually stored.
 * <p/>
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @author Karsten Blees
 * @since 4.0
 */
public class AsyncStore extends AbstractDelegatingStore {
   private static final Log log = LogFactory.getLog(AsyncStore.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final AtomicInteger threadId = new AtomicInteger(0);

   private final AsyncStoreConfig asyncStoreConfig;
   private final TransactionFactory txFactory;
   private Map<GlobalTransaction, List<? extends Modification>> transactions;

   private ExecutorService executor;
   private Thread coordinator;
   private int concurrencyLevel;
   private long shutdownTimeout;
   private String cacheName;

   private BufferLock stateLock;
   @GuardedBy("stateLock")
   private volatile State state;

   public AsyncStore(CacheStore delegate, AsyncStoreConfig asyncStoreConfig) {
      super(delegate);
      this.asyncStoreConfig = asyncStoreConfig;
      txFactory = new TransactionFactory();
      txFactory.init(false, false, false, false);
   }

   @Override
   public void init(CacheLoaderConfig config, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      super.init(config, cache, m);
      Configuration cacheCfg = cache != null ? cache.getCacheConfiguration() : null;
      concurrencyLevel = cacheCfg != null ? cacheCfg.locking().concurrencyLevel() : 16;
      long cacheStopTimeout = cacheCfg != null ? cacheCfg.transaction().cacheStopTimeout() : 30000;
      Long configuredAsyncStopTimeout = asyncStoreConfig.getShutdownTimeout();
      cacheName = cache != null ? cache.getName() : null;

      // Async store shutdown timeout cannot be bigger than
      // the overall cache stop timeout, so limit it accordingly.
      if (configuredAsyncStopTimeout >= cacheStopTimeout) {
         shutdownTimeout = Math.round(cacheStopTimeout * 0.90);
         log.asyncStoreShutdownTimeoutTooHigh(configuredAsyncStopTimeout, cacheStopTimeout, shutdownTimeout);
      } else {
         shutdownTimeout = configuredAsyncStopTimeout;
      }

      transactions = CollectionFactory.makeConcurrentMap(64, concurrencyLevel);
   }

   private State newState(boolean clear, State next) {
      ConcurrentMap<Object, Modification> map = CollectionFactory.makeConcurrentMap(64, concurrencyLevel);
      return new State(clear, map, next);
   }

   private void put(Modification mod, int count) {
      stateLock.writeLock(count);
      try {
         if (log.isTraceEnabled())
            log.tracef("Queue modification: %s", mod);

         state.put(mod);
      } finally {
         stateLock.writeUnlock();
      }
   }

   @Override
   public InternalCacheEntry load(Object key) throws CacheLoaderException {
      Modification mod = state.get(key);
      if (mod != null) {
         switch (mod.getType()) {
            case REMOVE:
            case CLEAR:
               return null;
            case STORE:
               InternalCacheEntry ice = ((Store) mod).getStoredEntry();
               if (ice.isExpired())
                  return null;
               return ice;
         }
      }

      return super.load(key);
   }

   @Override
   public boolean containsKey(Object key) throws CacheLoaderException {
      Modification mod = state.get(key);
      if (mod != null)
         return mod.getType() == Modification.Type.STORE;

      return super.containsKey(key);
   }

   private void loadKeys(State s, Set<Object> exclude, Set<Object> result) throws CacheLoaderException {
      // if not cleared, get keys from next State or the back-end store
      if (!s.clear) {
         State next = s.next;
         if (next != null)
            loadKeys(next, exclude, result);
         else
            result.addAll(super.loadAllKeys(exclude));
      }

      // merge keys of the current State
      for (Modification mod : s.modifications.values()) {
         switch (mod.getType()) {
            case STORE:
               Object key = ((Store) mod).getStoredEntry().getKey();
               if (exclude == null || !exclude.contains(key))
                  result.add(key);
               break;
            case REMOVE:
               result.remove(((Remove) mod).getKey());
               break;
         }
      }
   }

   @Override
   public Set<Object> loadAllKeys(Set<Object> keysToExclude) throws CacheLoaderException {
      Set<Object> result = new HashSet<Object>();
      loadKeys(state, keysToExclude, result);
      return result;
   }

   @Override
   public Set<InternalCacheEntry> loadAll() throws CacheLoaderException {
      return load(Integer.MAX_VALUE);
   }

   @Override
   public Set<InternalCacheEntry> load(int numEntries) throws CacheLoaderException {
      Set<InternalCacheEntry> result = new HashSet<InternalCacheEntry>();
      for (Object key : loadAllKeys(null)) {
         InternalCacheEntry entry = load(key);
         if (entry != null) {
            result.add(entry);
            if (result.size() == numEntries)
               return result;
         }
      }
      return result;
   }

   @Override
   public void store(InternalCacheEntry entry) {
      put(new Store(entry), 1);
   }

   @Override
   public void clear() {
      stateLock.writeLock(1);
      try {
         state = newState(true, state.next);
      } finally {
         stateLock.reset(1);
         stateLock.writeUnlock();
      }
   }

   @Override
   public boolean remove(Object key) {
      put(new Remove(key), 1);
      return true;
   }

   @Override
   public void removeAll(Set<Object> keys) throws CacheLoaderException {
      if (keys != null && !keys.isEmpty()) {
         List<Modification> mods = new ArrayList<Modification>(keys.size());
         for (Object key : keys)
            mods.add(new Remove(key));
         put(new ModificationsList(mods), mods.size());
      }
   }

   @Override
   public void prepare(List<? extends Modification> mods, GlobalTransaction tx, boolean isOnePhase)
         throws CacheLoaderException {
      if (isOnePhase) {
         enqueueModificationsList(mods);
      } else {
         transactions.put(tx, mods);
      }
   }

   @Override
   public void rollback(GlobalTransaction tx) {
      transactions.remove(tx);
   }

   @Override
   public void commit(GlobalTransaction tx) throws CacheLoaderException {
      enqueueModificationsList(transactions.remove(tx));
   }

   private void enqueueModificationsList(List<? extends Modification> mods) {
      // scan backwards to find the last CLEAR (anything before that can be discarded)
      int i = mods.size() - 1;
      for (; i >= 0; i--)
         if (mods.get(i).getType() == Modification.Type.CLEAR)
            break;
      // treat CLEAR specially
      if (i >= 0) {
         clear();
         mods = mods.subList(i + 1, mods.size());
      }
      // put the rest
      if (!mods.isEmpty())
         put(new ModificationsList(mods), mods.size());
   }

   @Override
   public void start() throws CacheLoaderException {
      log.debugf("Async cache loader starting %s", this);
      state = newState(false, null);
      stateLock = new BufferLock(asyncStoreConfig.getModificationQueueSize());

      super.start();

      int poolSize = asyncStoreConfig.getThreadPoolSize();
      executor = new ThreadPoolExecutor(0, poolSize, 120L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(),
            new ThreadFactory() {
               @Override
               public Thread newThread(Runnable r) {
                  Thread t = new Thread(r, "AsyncStoreProcessor-" + cacheName + "-" + threadId.getAndIncrement());
                  t.setDaemon(true);
                  return t;
               }
            });
      coordinator = new Thread(new AsyncStoreCoordinator(), "AsyncStoreCoordinator-" + cacheName);
      coordinator.setDaemon(true);
      coordinator.start();
   }

   @Override
   public void stop() throws CacheLoaderException {
      if (trace) log.tracef("Stop async store %s", this);
      stateLock.writeLock(1);
      state.stopped = true;
      stateLock.writeUnlock();
      try {
         coordinator.join(shutdownTimeout);
         if (coordinator.isAlive())
            log.error("Async store executor did not stop properly");
      } catch (InterruptedException e) {
         log.interruptedWaitingAsyncStorePush(e);
         Thread.currentThread().interrupt();
      }
      super.stop();
   }

   protected void applyModificationsSync(List<Modification> mods) throws CacheLoaderException {
      getDelegate().prepare(mods, txFactory.newGlobalTransaction(null, false), true);
   }

   private static class State {
      private static final Clear CLEAR = new Clear();

      /**
       * True if the state has been cleared before making modifications.
       */
      private final boolean clear;

      /**
       * Modifications to apply to the back-end CacheStore.
       */
      private final ConcurrentMap<Object, Modification> modifications;

      /**
       * Next state in the chain, initialized in constructor, may be set to <code>null</code>
       * asynchronously at any time.
       */
      private volatile State next;

      /**
       * True if the CacheStore has been stopped (i.e. this is the last state to process).
       */
      private volatile boolean stopped = false;

      /**
       * Number of worker threads that currently work with this instance.
       */
      private CountDownLatch workerThreads;

      private State(boolean clear, ConcurrentMap<Object, Modification> modMap, State next) {
         this.clear = clear;
         this.modifications = modMap;
         this.next = next;
         if (next != null)
            stopped = next.stopped;
      }

      /**
       * Gets the Modification for the specified key from this State object or chained (
       * <code>next</code>) State objects.
       *
       * @param key
       *           the key to look up
       * @return the Modification for the specified key, or <code>CLEAR</code> if the state was
       *         cleared, or <code>null</code> if the key is not in the state map
       */
      Modification get(Object key) {
         for (State state = this; state != null; state = state.next) {
            Modification mod = state.modifications.get(key);
            if (mod != null)
               return mod;
            else if (state.clear)
               return CLEAR;
         }
         return null;
      }

      /**
       * Adds the Modification(s) to the state map.
       *
       * @param mod
       *           the Modification to add, supports modification types STORE, REMOVE and LIST
       */
      void put(Modification mod) {
         if (stopped)
            throw new CacheException("AsyncStore stopped; no longer accepting more entries.");
         switch (mod.getType()) {
            case STORE:
               modifications.put(((Store) mod).getStoredEntry().getKey(), mod);
               break;
            case REMOVE:
               modifications.put(((Remove) mod).getKey(), mod);
               break;
            case LIST:
               for (Modification m : ((ModificationsList) mod).getList())
                  put(m);
               break;
            default:
               throw new IllegalArgumentException("Unknown modification type " + mod.getType());
         }
      }
   }

   /**
    * A custom reader-writer-lock combined with a bounded buffer size counter.
    * <p/>
    * Supports multiple concurrent writers and a single exclusive reader. This ensures that no more
    * data is being written to the current state when the AsyncStoreCoordinator thread hands the
    * data off to the back-end store.
    * <p/>
    * Additionally, {@link #writeLock(int)} blocks if the buffer is full, and {@link #readLock()}
    * blocks if no data is available.
    * <p/>
    * This lock implementation is <em>not</em> reentrant!
    */
   private static class BufferLock {
      /**
       * AQS state is the number of 'items' in the buffer. AcquireShared blocks if the buffer is
       * full (>= size).
       */
      private static class Counter extends AbstractQueuedSynchronizer {
         private static final long serialVersionUID = 1688655561670368887L;
         private final int size;

         Counter(int size) {
            this.size = size;
         }

         int add(int count) {
            for (;;) {
               int state = getState();
               if (compareAndSetState(state, state + count))
                  return state + count;
            }
         }

         protected int tryAcquireShared(int count) {
            for (;;) {
               int state = getState();
               if (state >= size)
                  return -1;
               if (compareAndSetState(state, state + count))
                  return state + count >= size ? 0 : 1;
            }
         }

         protected boolean tryReleaseShared(int state) {
            setState(state);
            return state < size;
         }
      }

      /**
       * AQS state is 0 if no data is available, 1 otherwise. AcquireShared blocks if no data is
       * available.
       */
      private static class Available extends AbstractQueuedSynchronizer {
         private static final long serialVersionUID = 6464514100313353749L;

         protected int tryAcquireShared(int unused) {
            return getState() > 0 ? 1 : -1;
         }

         protected boolean tryReleaseShared(int state) {
            setState(state > 0 ? 1 : 0);
            return state > 0;
         }
      }

      /**
       * Minimal non-reentrant read-write-lock. AQS state is number of concurrent shared locks, or 0
       * if unlocked, or -1 if locked exclusively.
       */
      private static class Sync extends AbstractQueuedSynchronizer {
         private static final long serialVersionUID = 2983687000985096017L;

         protected boolean tryAcquire(int unused) {
            if (!compareAndSetState(0, -1))
               return false;
            setExclusiveOwnerThread(Thread.currentThread());
            return true;
         }

         protected boolean tryRelease(int unused) {
            setExclusiveOwnerThread(null);
            setState(0);
            return true;
         }

         protected int tryAcquireShared(int unused) {
            for (;;) {
               int state = getState();
               if (state < 0)
                  return -1;
               if (compareAndSetState(state, state + 1))
                  return 1;
            }
         }

         protected boolean tryReleaseShared(int unused) {
            for (;;) {
               int state = getState();
               if (compareAndSetState(state, state - 1))
                  return true;
            }
         }
      }

      private final Sync sync;
      private final Counter counter;
      private final Available available;

      /**
       * Create a new BufferLock with the specified buffer size.
       *
       * @param size
       *           the buffer size
       */
      BufferLock(int size) {
         sync = new Sync();
         counter = size > 0 ? new Counter(size) : null;
         available = new Available();
      }

      /**
       * Acquires the write lock and consumes the specified amount of buffer space. Blocks if the
       * buffer is full or if the object is currently locked for reading.
       *
       * @param count
       *           number of items the caller intends to write
       */
      void writeLock(int count) {
         if (counter != null)
            counter.acquireShared(count);
         sync.acquireShared(1);
      }

      /**
       * Releases the write lock.
       */
      void writeUnlock() {
         sync.releaseShared(1);
         available.releaseShared(1);
      }

      /**
       * Acquires the read lock. Blocks if the buffer is empty or if the object is currently locked
       * for writing.
       */
      void readLock() {
         available.acquireShared(1);
         sync.acquire(1);
      }

      /**
       * Releases the read lock.
       */
      void readUnlock() {
         sync.release(1);
      }

      /**
       * Resets the buffer counter to the specified number.
       *
       * @param count
       *           number of available items in the buffer
       */
      void reset(int count) {
         if (counter != null)
            counter.releaseShared(count);
         available.releaseShared(count);
      }

      /**
       * Modifies the buffer counter by the specified value.
       *
       * @param count
       *           number of items to add to the buffer counter
       */
      void add(int count) {
         if (counter != null)
            count = counter.add(count);
         available.releaseShared(count);
      }
   }

   private class AsyncStoreCoordinator implements Runnable {

      @Override
      public void run() {
         LogFactory.pushNDC(cacheName, trace);
         try {
            for (;;) {
               State s, head, tail;
               s = state;
               if (shouldStop(s)) {
                  return;
               }

               stateLock.readLock();
               try {
                  s = state;
                  tail = s.next;
                  assert tail == null || tail.next == null : "State chain longer than 3 entries!";
                  state = head = newState(false, s);
               } finally {
                  stateLock.reset(0);
                  stateLock.readUnlock();
               }

               try {
                  if (s.clear) {
                     // clear() must be called synchronously, wait until background threads are done
                     if (tail != null)
                        tail.workerThreads.await();
                     getDelegate().clear();
                  }

                  List<Modification> mods;
                  if (tail != null) {
                     // if there's work in progress, push-back keys that are still in use to the head state
                     mods = new ArrayList<Modification>();
                     for (Map.Entry<Object, Modification> e : s.modifications.entrySet()) {
                        if (!tail.modifications.containsKey(e.getKey()))
                           mods.add(e.getValue());
                        else {
                           if (!head.clear && head.modifications.putIfAbsent(e.getKey(), e.getValue()) == null)
                              stateLock.add(1);
                           s.modifications.remove(e.getKey());
                        }
                     }
                  } else {
                     mods = new ArrayList<Modification>(s.modifications.values());
                  }

                  // distribute modifications evenly across worker threads
                  int threads = Math.min(mods.size(), asyncStoreConfig.getThreadPoolSize());
                  s.workerThreads = new CountDownLatch(threads);
                  if (threads > 0) {
                     // schedule background threads
                     int start = 0;
                     int quotient = mods.size() / threads;
                     int remainder = mods.size() % threads;
                     for (int i = 0; i < threads; i++) {
                        int end = start + quotient + (i < remainder ? 1 : 0);
                        executor.execute(new AsyncStoreProcessor(mods.subList(start, end), s));
                        start = end;
                     }
                     assert start == mods.size() : "Thread distribution is broken!";
                  }

                  // wait until background threads of previous round are done
                  if (tail != null) {
                     tail.workerThreads.await();
                     s.next = null;
                  }

                  // if this is the last state to process, wait for background threads, then quit
                  if (shouldStop(s)) {
                     s.workerThreads.await();
                     return;
                  }
               } catch (Exception e) {
                  if (log.isDebugEnabled())
                     log.debug("Failed to process async modifications", e);
               }
            }
         } finally {
            try {
               // Wait for existing workers to finish
               boolean workersTerminated = false;
               try {
                  executor.shutdown();
                  workersTerminated = executor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS);
               } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
               }
               if (!workersTerminated) {
                  // if the worker threads did not finish cleanly in the allotted time then we try to interrupt them to shut down
                  executor.shutdownNow();
               }
            } finally {
               LogFactory.popNDC(trace);
            }
         }
      }

      private boolean shouldStop(State s) {
         return s.stopped && s.modifications.isEmpty();
      }
   }

   private class AsyncStoreProcessor implements Runnable {
      private final List<Modification> modifications;
      private final State myState;

      AsyncStoreProcessor(List<Modification> modifications, State myState) {
         this.modifications = modifications;
         this.myState = myState;
      }

      @Override
      public void run() {
         // try 3 times to store the modifications
         retryWork(3);

         // decrement active worker threads and disconnect myState if this was the last one
         myState.workerThreads.countDown();
         if (myState.workerThreads.getCount() == 0)
            for (State s = state; s != null; s = s.next)
               if (s.next == myState)
                  s.next = null;
      }

      private void retryWork(int maxRetries) {
         for (int attempt = 0; attempt < maxRetries; attempt++) {
            if (attempt > 0 && log.isDebugEnabled())
               log.debugf("Retrying due to previous failure. %s attempts left.", maxRetries - attempt);

            try {
               AsyncStore.this.applyModificationsSync(modifications);
               return;
            } catch (Exception e) {
               if (log.isDebugEnabled())
                  log.debug("Failed to process async modifications", e);
            }
         }
         log.unableToProcessAsyncModifications(maxRetries);
      }
   }
}
