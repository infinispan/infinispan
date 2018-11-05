package org.infinispan.persistence.util;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;

import org.infinispan.filter.KeyFilter;
import org.infinispan.persistence.spi.MarshalledEntry;
import org.infinispan.persistence.spi.AdvancedCacheLoader;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.infinispan.util.function.CloseableSupplier;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A closeable supplier that provides a way to supply cache entries from a given persistence manager.  On the first
 * call to get this class will submit a task to collect all of the entries from the loader (or optionally a subset
 * provided a given {@link org.infinispan.filter.KeyFilter}).  A timeout value is required so that if a get blocks
 * for the given timeout it will throw a {@link TimeoutException}.
 * @author William Burns
 * @since 8.0
 * @deprecated This class is to be removed when {@link AdvancedCacheLoader#process(KeyFilter, AdvancedCacheLoader.CacheLoaderTask, Executor, boolean, boolean)} is removed
 */
@Deprecated
public class PersistenceManagerCloseableSupplier<K, V> implements CloseableSupplier<MarshalledEntry<K, V>> {
   private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());
   private static final boolean trace = log.isTraceEnabled();

   private final Executor executor;
   private final AdvancedCacheLoader<K, V> loader;
   private final Predicate<? super K> filter;
   private final boolean fetchValue;
   private final boolean fetchMetadata;
   private final BlockingQueue<MarshalledEntry<K, V>> queue;
   private final long timeout;
   private final TimeUnit unit;

   private final Lock closeLock = new ReentrantLock();
   private final Condition closeCondition = closeLock.newCondition();

   private boolean closed = false;
   private final AtomicReference<AdvancedCacheLoader.CacheLoaderTask<K, V>> taskRef = new AtomicReference<>();

   public PersistenceManagerCloseableSupplier(Executor executor, AdvancedCacheLoader<K, V> loader,
                                              Predicate<? super K> filter, boolean fetchValue,
                                              boolean fetchMetadata, long timeout,
                                              TimeUnit unit, int maxQueue) {
      this.executor = executor;
      this.loader = loader;
      this.filter = filter;
      this.fetchValue = fetchValue;
      this.fetchMetadata = fetchMetadata;
      this.timeout = timeout;
      this.unit = unit;
      this.queue = new ArrayBlockingQueue<>(maxQueue);
   }

   class SupplierCacheLoaderTask implements AdvancedCacheLoader.CacheLoaderTask<K, V> {

      @Override
      public void processEntry(MarshalledEntry<K, V> marshalledEntry, AdvancedCacheLoader.TaskContext taskContext)
              throws InterruptedException {
         if (!taskContext.isStopped()) {
            closeLock.lock();
            try {
               if (closed) {
                  taskContext.stop();
                  return;
               }
            } finally {
               closeLock.unlock();
            }
            // We do a read without acquiring lock
            boolean stop = closed;
            while (!stop) {
               // If we were able to offer a value this means someone took from the queue so let us come back around main
               // loop to offer all values we can
               // TODO: do some sort of batching here - to reduce wakeup contention ?
               if (queue.offer(marshalledEntry, 100, TimeUnit.MILLISECONDS)) {
                  closeLock.lock();
                  try {
                     // Wake up anyone waiting for a value
                     closeCondition.signalAll();
                  } finally {
                     closeLock.unlock();
                  }
                  break;
               }
               // If we couldn't offer an entry check if we were completed concurrently
               // We have to do in lock to ensure we see updated value properly
               closeLock.lock();
               try {
                  stop = closed;
               } finally {
                  closeLock.unlock();
               }
            }
         }
      }
   }

   @Override
   public MarshalledEntry<K, V> get() throws TimeoutException {
      // We do a regular get first just so subsequent get calls don't allocate the task.  If not we update the
      // ref to the new task atomically - the one who sets it starts the task
      if (taskRef.get() == null && taskRef.getAndUpdate((t) -> t == null ? new SupplierCacheLoaderTask() : t) == null) {
         AdvancedCacheLoader.CacheLoaderTask<K, V> task = taskRef.get();
         // TODO: unfortunately processOnAllStores requires 2 threads minimum unless using within thread executor - We
         // can't really use the persistence executor since we will block while waiting for additional work
         executor.execute(() -> {
            try {
               loader.process(filter != null ? filter::test : k -> true, task, new WithinThreadExecutor(), fetchValue, fetchMetadata);
            } finally {
               close();
            }
         });
      }
      MarshalledEntry<K, V> entry;
      boolean interrupted = false;
      // TODO: replace this with ForkJoinPool.ManagedBlocker
      while ((entry = queue.poll()) == null) {
         closeLock.lock();
         try {
            // If is possible that someone inserted a value and then acquired the close lock - thus we must recheck
            if ((entry = queue.poll()) != null || closed) {
               break;
            }
            long targetTime = System.nanoTime() + unit.toNanos(timeout);
            try {
               if (!closeCondition.await(targetTime - System.nanoTime(), TimeUnit.NANOSECONDS)) {
                  throw new TimeoutException("Couldn't retrieve entry an entry from store in allotted timeout: " + timeout
                          + " unit: " + unit);
               }
            } catch (InterruptedException e) {
               interrupted = true;
            }
         } finally {
            closeLock.unlock();
         }
      }
      if (interrupted) {
         Thread.currentThread().interrupt();
      }
      if (trace) {
         log.tracef("Returning entry: " + entry);
      }
      return entry;
   }

   @Override
   public void close() {
      closeLock.lock();
      try {
         closed = true;
         closeCondition.signalAll();
      } finally {
         closeLock.unlock();
      }
   }
}
