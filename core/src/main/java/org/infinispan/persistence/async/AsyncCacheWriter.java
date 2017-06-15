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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.Configuration;
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
      coordinator = coordinatorThreadFactory.newThread(new AsyncStoreCoordinator());
      coordinator.start();
   }

   @Override
   public void stop() {
      if (trace) log.tracef("Stop async store %s", this);
      stateLock.writeLock(0);
      stopped = true;
      stateLock.writeUnlock();
      try {
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
         if (!executor.awaitTermination(1, TimeUnit.SECONDS))
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

      @Override
      public void run() {
         LogFactory.pushNDC(cacheName, trace);
         try {
            for (;;) {
               final State s, head, tail;
               final boolean shouldStop;
               stateLock.readLock();
               try {
                  s = state.get();
                  shouldStop = stopped;
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
               result.add(new AsyncStoreProcessor(mods.subList(start, end), state));
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

      AsyncStoreProcessor(List<Modification> modifications, State myState) {
         this.modifications = modifications;
         this.myState = myState;
      }

      @Override
      public void run() {
         try {
            // try 3 times to store the modifications
            retryWork(3);

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
