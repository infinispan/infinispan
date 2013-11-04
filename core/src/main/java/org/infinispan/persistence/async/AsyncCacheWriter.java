package org.infinispan.persistence.async;

import net.jcip.annotations.GuardedBy;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.modifications.Modification;
import org.infinispan.persistence.modifications.Remove;
import org.infinispan.persistence.modifications.Store;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.support.DelegatingCacheWriter;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The AsyncCacheWriter is a delegating CacheStore that buffers changes and writes them asynchronously to
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
 * @author Mircea Markus
 * @since 4.0
 */
public class AsyncCacheWriter extends DelegatingCacheWriter {
   private static final Log log = LogFactory.getLog(AsyncCacheWriter.class);
   private static final boolean trace = log.isTraceEnabled();
   private static final AtomicInteger threadId = new AtomicInteger(0);

   private ExecutorService executor;
   private Thread coordinator;
   private int concurrencyLevel;
   private long shutdownTimeout;
   private String cacheName;

   protected BufferLock stateLock;
   @GuardedBy("stateLock")
   protected final AtomicReference<State> state = new AtomicReference<State>();

   protected AsyncStoreConfiguration asyncConfiguration;

   public AsyncCacheWriter(CacheWriter delegate) {
      super(delegate);
   }

   @Override
   public void init(InitializationContext ctx) {
      super.init(ctx);
      this.asyncConfiguration = ctx.getConfiguration().async();

      Cache cache = ctx.getCache();
      Configuration cacheCfg = cache != null ? cache.getCacheConfiguration() : null;
      concurrencyLevel = cacheCfg != null ? cacheCfg.locking().concurrencyLevel() : 16;
      long cacheStopTimeout = cacheCfg != null ? cacheCfg.transaction().cacheStopTimeout() : 30000;
      Long configuredAsyncStopTimeout = this.asyncConfiguration.shutdownTimeout();
      cacheName = cache != null ? cache.getName() : null;

      // Async store shutdown timeout cannot be bigger than
      // the overall cache stop timeout, so limit it accordingly.
      if (configuredAsyncStopTimeout >= cacheStopTimeout) {
         shutdownTimeout = Math.round(cacheStopTimeout * 0.90);
         log.asyncStoreShutdownTimeoutTooHigh(configuredAsyncStopTimeout, cacheStopTimeout, shutdownTimeout);
      } else {
         shutdownTimeout = configuredAsyncStopTimeout;
      }
   }

   @Override
   public void start() {
      log.debugf("Async cache loader starting %s", this);
      state.set(newState(false, null));
      stateLock = new BufferLock(asyncConfiguration.modificationQueueSize());

      int poolSize = asyncConfiguration.threadPoolSize();
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
   public void stop() {
      if (trace) log.tracef("Stop async store %s", this);
      stateLock.writeLock(1);
      state.get().stopped = true;
      stateLock.writeUnlock();
      try {
         coordinator.join(shutdownTimeout);
         if (coordinator.isAlive())
            log.errorAsyncStoreNotStopped();
      } catch (InterruptedException e) {
         log.interruptedWaitingAsyncStorePush(e);
         Thread.currentThread().interrupt();
      }
   }

   @Override
   public void write(MarshalledEntry entry) {
      put(new Store(entry.getKey(), entry), 1);
   }

   @Override
   public boolean delete(Object key) {
      put(new Remove(key), 1);
      return true;
   }

   protected void applyModificationsSync(List<Modification> mods) throws PersistenceException {
      for (Modification m : mods) {
         switch (m.getType()) {
            case STORE:
               actual.write(((Store) m).getStoredValue());
               break;
            case REMOVE:
               actual.delete(((Remove) m).getKey());
               break;
            default:
               throw new IllegalArgumentException("Unknown modification type " + m.getType());
         }
      }
   }


   State newState(boolean clear, State next) {
      ConcurrentMap<Object, Modification> map = CollectionFactory.makeConcurrentMap(64, concurrencyLevel);
      return new State(clear, map, next);
   }

   private void put(Modification mod, int count) {
      stateLock.writeLock(count);
      try {
         if (log.isTraceEnabled())
            log.tracef("Queue modification: %s", mod);

         state.get().put(mod);
      } finally {
         stateLock.writeUnlock();
      }
   }

   public AtomicReference<State> getState() {
      return state;
   }

   private class AsyncStoreCoordinator implements Runnable {

      @Override
      public void run() {
         LogFactory.pushNDC(cacheName, trace);
         try {
            for (;;) {
               State s, head, tail;
               s = state.get();
               if (shouldStop(s)) {
                  return;
               }

               stateLock.readLock();
               try {
                  s = state.get();
                  tail = s.next;
                  assert tail == null || tail.next == null : "State chain longer than 3 entries!";
                  head = newState(false, s);
                  state.set(head);
               } finally {
                  stateLock.reset(0);
                  stateLock.readUnlock();
               }

               try {
                  if (s.clear) {
                     // clear() must be called synchronously, wait until background threads are done
                     if (tail != null)
                        workerThreadsAwait(tail.workerThreads);
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
                  int threads = Math.min(mods.size(), asyncConfiguration.threadPoolSize());
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
                     workerThreadsAwait(tail.workerThreads);
                     s.next = null;
                  }

                  // if this is the last state to process, wait for background threads, then quit
                  if (shouldStop(s)) {
                     workerThreadsAwait(s.workerThreads);
                     return;
                  }
               } catch (InterruptedException e) {
                  log.asyncStoreCoordinatorInterrupted(e);
                  Thread.currentThread().interrupt();
               } catch (Exception e) {
                  log.unexpectedErrorInAsyncStoreCoordinator(e);
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

      private void workerThreadsAwait(CountDownLatch latch) throws InterruptedException {
         boolean await = latch.await(shutdownTimeout, TimeUnit.MILLISECONDS);
         if (!await)
            throw log.waitingForWorkerThreadsFailed(latch);
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
            for (State s = state.get(); s != null; s = s.next)
               if (s.next == myState)
                  s.next = null;
      }

      private void retryWork(int maxRetries) {
         for (int attempt = 0; attempt < maxRetries; attempt++) {
            if (attempt > 0 && log.isDebugEnabled())
               log.debugf("Retrying due to previous failure. %s attempts left.", maxRetries - attempt);

            try {
               AsyncCacheWriter.this.applyModificationsSync(modifications);
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
