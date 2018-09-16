package org.infinispan.persistence.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.PersistenceConfiguration;
import org.infinispan.factories.threads.DefaultThreadFactory;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.modifications.Modification;
import org.infinispan.persistence.modifications.ModificationsList;
import org.infinispan.persistence.modifications.Remove;
import org.infinispan.persistence.modifications.Store;
import org.infinispan.persistence.spi.CacheWriter;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.persistence.support.DelegatingCacheWriter;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import net.jcip.annotations.GuardedBy;

/**
 * The AsyncCacheWriter is a delegating CacheStore that buffers changes and writes them asynchronously to
 * the underlying CacheStore.
 * <p>
 * Read operations are done synchronously, taking into account the current state of buffered changes.
 * <p>
 * There is no provision for exception handling for problems encountered with the underlying store
 * during a write operation, and the exception is just logged.
 * <p>
 * When configuring the loader, use the following element:
 * <p>
 * <code> &lt;async enabled="true" /&gt; </code>
 * <p>
 * to define whether cache loader operations are to be asynchronous. If not specified, a cache loader operation is
 * assumed synchronous and this decorator is not applied.
 * <p>
 * Write operations affecting same key are now coalesced so that only the final state is actually stored.
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

   private ExecutorService executor;
   private Thread coordinator;
   private int concurrencyLevel;
   private String cacheName;
   private String nodeName;

   protected BufferLock stateLock;
   @GuardedBy("stateLock")
   protected final AtomicReference<State> state = new AtomicReference<>();
   @GuardedBy("stateLock")
   private boolean stopped;

   private final Lock availabilityLock = new ReentrantLock();
   private final Condition availability = availabilityLock.newCondition();
   @GuardedBy("availabilityLock")
   private volatile boolean delegateAvailable = true;

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
      cacheName = cache != null ? cache.getName() : null;
      nodeName = cache != null ? cache.getCacheManager().getCacheManagerConfiguration().transport().nodeName() : null;
   }

   @Override
   public void start() {
      log.debugf("Async cache loader starting %s", this);
      state.set(newState(false, null));
      stopped = false;
      stateLock = new BufferLock(asyncConfiguration.modificationQueueSize());

      // Create a thread pool with unbounded work queue, so that all work is accepted and eventually
      // executed. A bounded queue could throw RejectedExecutionException and thus lose data.
      int poolSize = asyncConfiguration.threadPoolSize();
      DefaultThreadFactory processorThreadFactory =
            new DefaultThreadFactory(null, Thread.NORM_PRIORITY, DefaultThreadFactory.DEFAULT_PATTERN, nodeName,
                                     "AsyncStoreProcessor");
      executor = new ThreadPoolExecutor(poolSize, poolSize, 120L, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                                        processorThreadFactory);
      ((ThreadPoolExecutor) executor).allowCoreThreadTimeOut(true);

      DefaultThreadFactory coordinatorThreadFactory =
            new DefaultThreadFactory(null, Thread.NORM_PRIORITY, DefaultThreadFactory.DEFAULT_PATTERN, nodeName,
                                     "AsyncStoreCoordinator");

      coordinator = coordinatorThreadFactory.newThread(new AsyncStoreCoordinator(asyncConfiguration.failSilently()));
      coordinator.start();
   }

   @Override
   public void stop() {
      if (trace) log.tracef("Stop async store %s", this);
      stateLock.writeLock(0);
      stopped = true;
      stateLock.writeUnlock();

      try {
         if (!asyncConfiguration.failSilently() && !delegateAvailable) {
            // The delegate store is unavailable, therefore we must interrupt the AsyncStoreProcessor(s) threads
            // as they will be awaiting an availability signal
            coordinator.interrupt();
            executor.shutdownNow();
         } else {
            // It is safe to wait without timeout because the thread pool uses an unbounded work queue (i.e.
            // all work handed to the pool will be accepted and eventually executed) and AsyncStoreProcessors
            // decrement the workerThreads latch in a finally block (i.e. even if the back-end store throws
            // java.lang.Error). The coordinator thread can only block forever if the back-end's write() /
            // remove() methods block, but this is no different from PassivationManager.stop() being blocked
            // in a synchronous call to write() / remove().
            coordinator.join();
            // The coordinator thread waits for AsyncStoreProcessor threads to count down their latch (nearly
            // at the end). Thus the threads should have terminated or terminate instantly.
            executor.shutdown();
         }
         if (!executor.awaitTermination(1, TimeUnit.SECONDS))
            log.errorAsyncStoreNotStopped();
      } catch (InterruptedException e) {
         log.interruptedWaitingAsyncStorePush(e);
         Thread.currentThread().interrupt();
      }
   }

   @Override
   public boolean isAvailable() {
      if (stopped)
         return false;

      if (asyncConfiguration.failSilently())
         return true;

      boolean available = false;
      try {
         available = actual.isAvailable();
      } catch (Throwable t) {
         // We swallow the exception here so that modifications can still be added to the queue if there is capacity
         log.debugf("Error encountered when calling isAvailable on %s: %s", actual, t);
      }
      availabilityLock.lock();
      try {
         if (available != delegateAvailable) {
            delegateAvailable = available;
            if (delegateAvailable)
               availability.signalAll();
         }
      } finally {
         availabilityLock.unlock();
      }
      // Available if actual == available || actual != available and queue has capacity
      // Worst case, writeBatch comes in before isAvailable is called by the PersistenceManager, in which case the batch
      // will wait on writeLock until the stateLock is reset when the queue is finally flushed
      return delegateAvailable || stateLock.hasCapacity();
   }

   @Override
   public void write(MarshalledEntry entry) {
      put(new Store(entry.getKey(), entry), 1);
   }

   @Override
   public void writeBatch(Iterable entries) {
      putAll(
            StreamSupport.stream((Spliterator<MarshalledEntry>) entries.spliterator(), false)
                  .map(me -> new Store(me.getKey(), me))
                  .collect(Collectors.toList())
      );
   }

   @Override
   public void deleteBatch(Iterable keys) {
      putAll(
            StreamSupport.stream((Spliterator<Object>) keys.spliterator(), false)
                  .map(Remove::new)
                  .collect(Collectors.toList())
      );
   }

   @Override
   public boolean delete(Object key) {
      put(new Remove(key), 1);
      return true;
   }

   protected void applyModificationsSync(List<Modification> mods) throws PersistenceException {
      actual.writeBatch(
            () -> StreamSupport.stream(Spliterators.spliterator(mods, Spliterator.NONNULL), false)
                  .filter(m -> m.getType() == Modification.Type.STORE)
                  .map(Store.class::cast)
                  .map(Store::getStoredValue)
                  .iterator()
      );

      actual.deleteBatch(
            () -> StreamSupport.stream(Spliterators.spliterator(mods, Spliterator.NONNULL), false)
                  .filter(m -> m.getType() == Modification.Type.REMOVE)
                  .map(Remove.class::cast)
                  .map(Remove::getKey)
                  .iterator()
      );
   }

   protected State newState(boolean clear, State next) {
      ConcurrentMap<Object, Modification> map = CollectionFactory.makeConcurrentMap(64, concurrencyLevel);
      return new State(clear, map, next);
   }

   void assertNotStopped() throws CacheException {
      if (stopped)
         throw new CacheException("AsyncCacheWriter stopped; no longer accepting more entries.");
   }

   private void put(Modification mod, int count) {
      stateLock.writeLock(count);
      try {
         if (trace)
            log.tracef("Queue modification: %s", mod);

         assertNotStopped();
         state.get().put(mod);
      } finally {
         stateLock.writeUnlock();
      }
   }

   private void putAll(List<Modification> mods) {
      stateLock.writeLock(mods.size());
      try {
         state.get().put(new ModificationsList(mods));
      } finally {
         stateLock.writeUnlock();
      }
   }

   public AtomicReference<State> getState() {
      return state;
   }

   protected void clearStore() {
      // No-op, not supported for async
   }

   private class AsyncStoreCoordinator implements Runnable {

      final boolean failSilently;

      AsyncStoreCoordinator(boolean failSilently) {
         this.failSilently = failSilently;
      }

      @Override
      public void run() {
         LogFactory.pushNDC(cacheName, trace);
         try {
            for (;;) {
               final State s, head, tail;
               final boolean shouldStop;
               stateLock.readLock();
               if (!failSilently) {
                  availabilityLock.lock();
                  try {
                     // If the delegate is unavailable, relinquish the readLock and await for the delegate to become available
                     if (!delegateAvailable) {
                        stateLock.readUnlock();
                        availability.await();

                        // Restart the loop so that the readLock is reacquired
                        continue;
                     }
                  } catch (InterruptedException e) {
                     log.debugf("%s interrupted: %s", this, e);
                     Thread.currentThread().interrupt();
                     return;
                  } finally {
                     availabilityLock.unlock();
                  }
               }

               try {
                  s = state.get();
                  shouldStop = stopped;
                  tail = s.next;
                  assert tail == null || tail.next == null : "State chain longer than 3 entries!";
                  head = newState(false, s);
                  state.set(head);
                  stateLock.reset(0);
               } finally {
                  stateLock.readUnlock();
               }

               try {
                  if (s.clear) {
                     // clear() must be called synchronously, wait until background threads are done
                     if (tail != null)
                        tail.workerThreads.await();

                     clearStore();
                  }

                  final List<Modification> mods = new ArrayList<>(s.modifications.size());
                  final List<Modification> deferredMods = new ArrayList<>();
                  if (tail != null && tail.workerThreads.getCount() > 0) {
                     // sort out modifications that are still in use by tail's AsyncStoreProcessors
                     for (Map.Entry<Object, Modification> e : s.modifications.entrySet()) {
                        if (!tail.modifications.containsKey(e.getKey()))
                           mods.add(e.getValue());
                        else
                           deferredMods.add(e.getValue());
                     }
                  } else {
                     mods.addAll(s.modifications.values());
                  }

                  // create AsyncStoreProcessors
                  final List<AsyncStoreProcessor> procs = createProcessors(s, mods);
                  final List<AsyncStoreProcessor> deferredProcs = createProcessors(s, deferredMods);
                  s.workerThreads = new CountDownLatch(procs.size() + deferredProcs.size());

                  // schedule AsyncStoreProcessors that don't conflict with tail's processors
                  for (AsyncStoreProcessor processor : procs)
                     executor.execute(processor);

                  // wait until background threads of previous round are done
                  if (tail != null) {
                     tail.workerThreads.await();
                     s.next = null;
                  }

                  // schedule remaining AsyncStoreProcessors
                  for (AsyncStoreProcessor processor : deferredProcs)
                     executor.execute(processor);

                  // if this is the last state to process, wait for background threads, then quit
                  if (shouldStop) {
                     s.workerThreads.await();
                     return;
                  }
               } catch (Exception e) {
                  log.unexpectedErrorInAsyncStoreCoordinator(e);
               }
            }
         } finally {
            LogFactory.popNDC(trace);
         }
      }

      private List<AsyncStoreProcessor> createProcessors(State state, List<Modification> mods) {
         List<AsyncStoreProcessor> result = new ArrayList<>();
         // distribute modifications evenly across worker threads
         int threads = Math.min(mods.size(), asyncConfiguration.threadPoolSize());
         if (threads > 0) {
            // create background threads
            int start = 0;
            int quotient = mods.size() / threads;
            int remainder = mods.size() % threads;
            for (int i = 0; i < threads; i++) {
               int end = start + quotient + (i < remainder ? 1 : 0);
               result.add(new AsyncStoreProcessor(mods.subList(start, end), state, failSilently));
               start = end;
            }
            assert start == mods.size() : "Thread distribution is broken!";
         }
         return result;
      }
   }

   private class AsyncStoreProcessor implements Runnable {
      private final List<Modification> modifications;
      private final State myState;
      private final boolean failSilently;
      private final PersistenceConfiguration configuration;

      AsyncStoreProcessor(List<Modification> modifications, State myState, boolean failSilently) {
         this.modifications = modifications;
         this.myState = myState;
         this.failSilently = failSilently;
         this.configuration = ctx.getCache().getCacheConfiguration().persistence();
      }

      @Override
      public void run() {
         try {
            retryWork(configuration.connectionAttempts());
         } finally {
            // decrement active worker threads and disconnect myState if this was the last one
            myState.workerThreads.countDown();
            if (myState.workerThreads.getCount() == 0 && myState.next == null)
               for (State s = state.get(); s != null; s = s.next)
                  if (s.next == myState)
                     s.next = null;
         }
      }

      private void retryWork(int maxRetries) {
         // Even with !failSilently we only try maxRetries times as it's possible that the failure is not due to store availability and this
         // prevents us repeating the failed operation indefinitely
         for (int attempt = 0; attempt < maxRetries; attempt++) {
            if (attempt > 0 && log.isDebugEnabled())
               log.debugf("Retrying due to previous failure. %s attempts left.", maxRetries - attempt);

            try {
               if (!failSilently) {
                  availabilityLock.lock();
                  try {
                     // It's necessary to check the delegate's availability here as it's possible that it changed after
                     // the AsyncStoreProcessor was created
                     if (!delegateAvailable) {
                        if (stopped) {
                           log.debugf("Failed to write async modifications to %s as the store is unavailable and stop() was called", actual);
                           return;
                        }

                        availability.await();
                     }
                  } catch (InterruptedException e) {
                     log.debugf("%s interrupted: %s", this, e);
                     Thread.currentThread().interrupt();
                     break;
                  } finally {
                     availabilityLock.unlock();
                  }
               }
               AsyncCacheWriter.this.applyModificationsSync(modifications);
               return;
            } catch (Exception e) {
               if (log.isDebugEnabled())
                  log.debug("Failed to process async modifications", e);

               if (!failSilently) {
                  try {
                     // Wait for availabilityInterval time to ensure that before the next attempt the delegate's availability
                     // flag will have been updated and availability.await will be reached
                     Thread.sleep(configuration.availabilityInterval());
                  } catch (InterruptedException ie) {
                     Thread.currentThread().interrupt();
                     break;
                  }
               }
            }
         }
         log.unableToProcessAsyncModifications(maxRetries);
      }
   }
}
