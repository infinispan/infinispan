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
import org.infinispan.config.Configuration;
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
import org.infinispan.util.concurrent.ConcurrentMapFactory;
import org.infinispan.util.concurrent.locks.containers.ReentrantPerEntryLockContainer;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The AsyncStore is a delegating CacheStore that extends AbstractDelegatingStore, overriding methods to that should not
 * just delegate the operation to the underlying store.
 * <p/>
 * Read operations are done synchronously, while write operations are done asynchronously.  There is no provision for
 * exception handling for problems encountered with the underlying store during a write operation, and the exception is
 * just logged.
 * <p/>
 * When configuring the loader, use the following element:
 * <p/>
 * <code> &lt;async enabled="true" /&gt; </code>
 * <p/>
 * to define whether cache loader operations are to be asynchronous.  If not specified, a cache loader operation is
 * assumed synchronous and this decorator is not applied.
 * <p/>
 * Write operations affecting same key are now coalesced so that only the final state is actually stored.
 * <p/>
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @author Sanne Grinovero
 * @since 4.0
 */
public class AsyncStore extends AbstractDelegatingStore {
   private static final Log log = LogFactory.getLog(AsyncStore.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final AtomicInteger threadId = new AtomicInteger(0);
   private final AtomicBoolean stopped = new AtomicBoolean(true);
   
   private final AsyncStoreConfig asyncStoreConfig;
   private Map<GlobalTransaction, List<? extends Modification>> transactions;
   
   /**
    * This is used as marker to shutdown the AsyncStoreCoordinator
    */
   private static final Modification QUIT_SIGNAL = new Clear();
   
   /**
    * clear() is performed in sync by the one thread of storeCoordinator, while blocking all
    * other threads interacting with the decorated store.
    */
   private final ReadWriteLock clearAllLock = new ReentrantReadWriteLock();
   private final Lock clearAllReadLock = clearAllLock.readLock();
   private final Lock clearAllWriteLock = clearAllLock.writeLock();
   private final Lock stateMapLock = new ReentrantLock();
   
   ExecutorService executor;
   private int concurrencyLevel;
   @GuardedBy("stateMapLock")
   protected ConcurrentMap<Object, Modification> state;
   private ReleaseAllLockContainer lockContainer;
   private LinkedBlockingQueue<Modification> changesDeque;
   public volatile boolean lastAsyncProcessorShutsDownExecutor = false;
   private long shutdownTimeout;
   private String cacheName;

   public AsyncStore(CacheStore delegate, AsyncStoreConfig asyncStoreConfig) {
      super(delegate);
      this.asyncStoreConfig = asyncStoreConfig;
   }

   @Override
   public void init(CacheLoaderConfig config, Cache<?, ?> cache, StreamingMarshaller m) throws CacheLoaderException {
      super.init(config, cache, m);
      changesDeque = new LinkedBlockingQueue<Modification>(asyncStoreConfig.getModificationQueueSize());
      Configuration cacheCfg = cache != null ? cache.getConfiguration() : null;
      concurrencyLevel = cacheCfg != null ? cacheCfg.getConcurrencyLevel() : 16;
      int cacheStopTimeout = cacheCfg != null ? cacheCfg.getCacheStopTimeout() : 30000;
      Long configuredAsyncStopTimeout = asyncStoreConfig.getShutdownTimeout();
      cacheName = cacheCfg != null ? cacheCfg.getName() : null;

      // Async store shutdown timeout cannot be bigger than
      // the overall cache stop timeout, so limit it accordingly.
      if (configuredAsyncStopTimeout >= cacheStopTimeout) {
         shutdownTimeout = Math.round(cacheStopTimeout * 0.90);
         log.asyncStoreShutdownTimeoutTooHigh(configuredAsyncStopTimeout, cacheStopTimeout, shutdownTimeout);
      } else {
         shutdownTimeout = configuredAsyncStopTimeout;
      }

      lockContainer = new ReleaseAllLockContainer(concurrencyLevel);
      transactions = ConcurrentMapFactory.makeConcurrentMap(64, concurrencyLevel);
   }

   @Override
   public void store(InternalCacheEntry ed) {
      enqueue(new Store(ed));
   }

   @Override
   public boolean remove(Object key) {
      enqueue(new Remove(key));
      return true;
   }

   @Override
   public void clear() {
      Clear clear = new Clear();
      checkNotStopped(); //check we can change the changesDeque
      changesDeque.clear();
      enqueue(clear);
   }

   @Override
   public void prepare(List<? extends Modification> mods, GlobalTransaction tx, boolean isOnePhase) throws CacheLoaderException {
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
      List<? extends Modification> list = transactions.remove(tx);
      enqueueModificationsList(list);
   }
   
   protected void enqueueModificationsList(List<? extends Modification> mods) {
      if (mods != null && !mods.isEmpty()) {
         enqueue(new ModificationsList(mods));
      }
   }

   @Override
   public void start() throws CacheLoaderException {
      state = newStateMap();
      log.debugf("Async cache loader starting %s", this);
      stopped.set(false);
      lastAsyncProcessorShutsDownExecutor = false;
      super.start();
      int poolSize = asyncStoreConfig.getThreadPoolSize();
      executor = new ThreadPoolExecutor(poolSize, poolSize, 0L, TimeUnit.MILLISECONDS,
               // note the use of poolSize+1 as maximum workingQueue together with DiscardPolicy:
               // this way when a new AsyncProcessor is started unnecessarily we discard it
               // before it takes locks to perform no work
               // this way we save memory from the executor queue, CPU, and also avoid
               // any possible RejectedExecutionException.
               new LinkedBlockingQueue<Runnable>(poolSize + 1),
               new ThreadFactory() {
                  @Override
                  public Thread newThread(Runnable r) {
                     Thread t = new Thread(r, "CoalescedAsyncStore-" + threadId.getAndIncrement());
                     t.setDaemon(true);
                     return t;
                  }
               },
               new ThreadPoolExecutor.DiscardPolicy()
         );
      startStoreCoordinator();
   }

   private void startStoreCoordinator() {
      ExecutorService storeCoordinator = Executors.newFixedThreadPool(1);
      storeCoordinator.execute( new AsyncStoreCoordinator() );
      storeCoordinator.shutdown();
   }

   @Override
   public void stop() throws CacheLoaderException {
      stopped.set(true);
      try {
         changesDeque.put(QUIT_SIGNAL);
         boolean finished = executor.awaitTermination(shutdownTimeout, TimeUnit.MILLISECONDS);
         if (!finished) log.error("Async store executor did not stop properly");
      } catch (InterruptedException e) {
         log.interruptedWaitingAsyncStorePush(e);
         Thread.currentThread().interrupt();
      }
      super.stop();
   }

   protected void applyModificationsSync(ConcurrentMap<Object, Modification> mods) throws CacheLoaderException {
      Set<Map.Entry<Object, Modification>> entries = mods.entrySet();
      for (Map.Entry<Object, Modification> entry : entries) {
         Modification mod = entry.getValue();
         switch (mod.getType()) {
            case STORE:
               super.store(((Store) mod).getStoredEntry());
               break;
            case REMOVE:
               super.remove(entry.getKey());
               break;
            default:
               throw new IllegalArgumentException("Unexpected modification type " + mod.getType());
         }
      }
   }
   
   protected boolean applyClear() {
      try {
         super.clear();
         return true;
      } catch (CacheLoaderException e) {
         log.errorClearinAsyncStore(e);
         return false;
      }
   }
   
   protected void delegatePurgeExpired() {
      try {
         super.purgeExpired();
      } catch (CacheLoaderException e) {
         log.errorPurgingAsyncStore(e);
      }
   }

   private void enqueue(Modification mod) {
      try {
         checkNotStopped();
         if (trace) log.tracef("Enqueuing modification %s", mod);
         changesDeque.put(mod);
      } catch (Exception e) {
         throw new CacheException("Unable to enqueue asynchronous task", e);
      }
   }

   private void checkNotStopped() {
      if (stopped.get()) {
         throw new CacheException("AsyncStore stopped; no longer accepting more entries.");
      }
   }

   private void acquireLock(Lock lock) {
      try {
         if (!lock.tryLock(asyncStoreConfig.getFlushLockTimeout(), TimeUnit.MILLISECONDS))
            throw new CacheException("Unable to acquire lock on update map");
      } catch (InterruptedException ie) {
         // restore interrupted status
         Thread.currentThread().interrupt();
      }
   }

   /**
    * Processes modifications taking the latest updates from a state map.
    */
   class AsyncProcessor implements Runnable {
      private final Set<Object> lockedKeys = new HashSet<Object>();
      boolean runAgainAfterWaiting = false;

      @Override
      public void run() {
         LogFactory.pushNDC(cacheName, trace);
         try {
            clearAllReadLock.lock();
            try {
               innerRun();
            } catch (Throwable t) {
               runAgainAfterWaiting = false;
               log.unexpectedErrorInAsyncProcessor(t);
            } finally {
               clearAllReadLock.unlock();
            }
            if (runAgainAfterWaiting) {
               try {
                  Thread.sleep(10);
               } catch (InterruptedException e) {
                  // just speedup ignoring more sleep but still make sure to store all data
               }
               ensureMoreWorkIsHandled();
            }
         } finally {
            LogFactory.popNDC(trace);
         }
      }
      
      private void innerRun() {
         final ConcurrentMap<Object, Modification> swap;
         if (trace) log.trace("Checking for modifications");
         try {
            acquireLock(stateMapLock);
            try {
               swap = state;
               state = newStateMap();

               // This needs to be done within the stateMapLock section, because if a key is in use,
               // we need to put it back in the state
               // map for later processing and we don't wanna do it in such way that we override a
               // newer value that might
               // have been taken already for processing by another instance of this same code.
               // AsyncStoreCoordinator doesn't need to acquired the same lock as values put by it
               // will never be overwritten (putIfAbsent below)
               for (Object key : swap.keySet()) {
                  if (trace) log.tracef("Going to process mod key: %s", key);
                  boolean acquired;
                  try {
                     acquired = lockContainer.acquireLock(null, key, 0, TimeUnit.NANOSECONDS) != null;
                  } catch (InterruptedException e) {
                     log.interruptedAcquiringLock(0, e);
                     Thread.currentThread().interrupt();
                     return;
                  }
                  if (trace)
                     log.tracef("Lock for key %s was acquired=%s", key, acquired);
                  if (!acquired) {
                     Modification prev = swap.remove(key);
                     Modification didPut = state.putIfAbsent(key, prev); // don't overwrite more recently put work
                     if (didPut == null) {
                        // otherwise a new job is being spawned by the arbiter, so no need to create
                        // a new worker
                        runAgainAfterWaiting = true;
                     }
                  } else {
                     lockedKeys.add(key);
                  }
               }
            } finally {
               stateMapLock.unlock();
            }

            if (swap.isEmpty()) {
               if (lastAsyncProcessorShutsDownExecutor && !runAgainAfterWaiting) {
                  executor.shutdown();
               }
            } else {
               if (trace)
                  log.tracef("Apply %s modifications", swap.size());
               int maxRetries = 3;
               int attemptNumber = 0;
               boolean successful;
               do {
                  if (attemptNumber > 0 && log.isDebugEnabled())
                     log.debugf("Retrying due to previous failure. %s attempts left.", maxRetries - attemptNumber);
                  successful = put(swap);
                  attemptNumber++;
               } while (!successful && attemptNumber <= maxRetries);

               if (!successful)
                  log.unableToProcessAsyncModifications(maxRetries);

            }
         } finally {
            lockContainer.releaseLocks(lockedKeys);
            lockedKeys.clear();
         }
      }

      boolean put(ConcurrentMap<Object, Modification> mods) {
         try {
            AsyncStore.this.applyModificationsSync(mods);
            return true;
         } catch (Exception e) {
            if (log.isDebugEnabled()) log.debug("Failed to process async modifications", e);
            return false;
         }
      }
   }

   private ConcurrentMap<Object, Modification> newStateMap() {
      return ConcurrentMapFactory.makeConcurrentMap(64, concurrencyLevel);
   }
   
   private static class ReleaseAllLockContainer extends ReentrantPerEntryLockContainer {
      private ReleaseAllLockContainer(int concurrencyLevel) {
         super(concurrencyLevel);
      }

      void releaseLocks(Set<Object> keys) {
         for (Object key : keys) {
            if (trace) log.tracef("Release lock for key %s", key);
            releaseLock(null, key);
         }
      }
   }

   private void ensureMoreWorkIsHandled() {
           executor.execute(new AsyncProcessor());
   }
   
   private class AsyncStoreCoordinator implements Runnable {

      @Override
      public void run() {
         LogFactory.pushNDC(cacheName, trace);
         try {
            while (true) {
               try {
                  Modification take = changesDeque.take();
                  if (take == QUIT_SIGNAL) {
                     lastAsyncProcessorShutsDownExecutor = true;
                     ensureMoreWorkIsHandled();
                     return;
                  }
                  else {
                     handleSafely(take);
                  }
               } catch (InterruptedException e) {
                  log.asyncStoreCoordinatorInterrupted(e);
                  return;
               } catch (Throwable t) {
                  log.unexpectedErrorInAsyncStoreCoordinator(t);
               }
            }
         } finally {
            LogFactory.popNDC(trace);
         }
      }

      private void handleSafely(Modification mod) {
         try {
            if (trace) log.tracef("taking from modification queue: %s", mod);
            handle(mod, false);
         } catch (Exception e) {
            log.errorModifyingAsyncStore(e);
         }
      }

      private void handle(Modification mod, boolean nested) {
         boolean asyncProcessorNeeded = false;
         switch (mod.getType()) {
            case STORE:
               Store store = (Store) mod;
               stateMapLock.lock();
               state.put(store.getStoredEntry().getKey(), store);
               stateMapLock.unlock();
               asyncProcessorNeeded = true;
               break;
            case REMOVE:
               Remove remove = (Remove) mod;
               stateMapLock.lock();
               state.put(remove.getKey(), remove);
               stateMapLock.unlock();
               asyncProcessorNeeded = true;
               break;
            case CLEAR:
               performClear();
               break;
            case PURGE_EXPIRED:
               delegatePurgeExpired();
               break;
            case LIST:
               applyModificationsList((ModificationsList) mod);
               asyncProcessorNeeded = true;
               break;
            default:
               throw new IllegalArgumentException("Unexpected modification type " + mod.getType());
         }
         if (asyncProcessorNeeded && !nested) {
            // we know when it's possible for some work to be done, starting short-lived
            // AsyncProcessor(s) simplifies shutdown process.
             ensureMoreWorkIsHandled();
         }
      }

      private void applyModificationsList(ModificationsList mod) {
         for (Modification m : mod.getList()) {
            handle(m, true);
         }
      }

      private void performClear() {
         state.clear(); // cancel any other scheduled changes
         clearAllWriteLock.lock(); // ensure no other tasks concurrently working
         try {
            // to acquire clearAllWriteLock we might have had to wait for N AsyncProcessor to have finished
            // (as they have to release all clearAllReadLock),
            // so as they might have put back some work to the state map, clear the state map again inside the writeLock:
            state.clear();
            if (trace) log.trace("Performed clear operation");
            int maxRetries = 3;
            int attemptNumber = 0;
            boolean successful = false;
            do {
               if (attemptNumber > 0 && log.isDebugEnabled())
                  log.debugf("Retrying clear() due to previous failure. %s attempts left.", maxRetries - attemptNumber);
               successful = applyClear();
               attemptNumber++;
            } while (!successful && attemptNumber <= maxRetries);
            if (!successful) {
               log.unableToClearAsyncStore();
            }
         } finally {
            clearAllWriteLock.unlock();
         }
      }

   }
}
