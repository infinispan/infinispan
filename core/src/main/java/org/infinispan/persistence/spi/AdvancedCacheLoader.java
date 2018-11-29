package org.infinispan.persistence.spi;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.persistence.util.PersistenceManagerCloseableSupplier;
import org.infinispan.util.CloseableSuppliedIterator;
import org.infinispan.util.KeyValuePair;
import org.reactivestreams.Publisher;

import io.reactivex.Flowable;
import net.jcip.annotations.ThreadSafe;

/**
 * A specialised extension of the {@link CacheLoader} interface that allows processing parallel iteration over the
 * existing entries.
 *
 * @author Mircea Markus
 * @since 6.0
 */
@ThreadSafe
public interface AdvancedCacheLoader<K, V> extends CacheLoader<K, V> {

   /**
    * Iterates in parallel over the entries in the storage using the threads from the <b>executor</b> pool. For each
    * entry the {@link CacheLoaderTask#processEntry(MarshalledEntry, TaskContext)} is
    * invoked. Before passing an entry to the callback task, the entry should be validated against the <b>filter</b>.
    * Implementors should build an {@link TaskContext} instance (implementation) that is fed to the {@link
    * CacheLoaderTask} on every invocation. The {@link CacheLoaderTask} might invoke {@link
    * org.infinispan.persistence.spi.AdvancedCacheLoader.TaskContext#stop()} at any time, so implementors of this method
    * should verify TaskContext's state for early termination of iteration. The method should only return once the
    * iteration is complete or as soon as possible in the case TaskContext.stop() is invoked.
    *
    * @param filter        to validate which entries should be feed into the task. Might be null.
    * @param task          callback to be invoked in parallel for each stored entry that passes the filter check
    * @param executor      an external thread pool to be used for parallel iteration
    * @param fetchValue    whether or not to fetch the value from the persistent store. E.g. if the iteration is
    *                      intended only over the key set, no point fetching the values from the persistent store as
    *                      well
    * @param fetchMetadata whether or not to fetch the metadata from the persistent store. E.g. if the iteration is
    *                      intended only ove the key set, then no pint fetching the metadata from the persistent store
    *                      as well
    * @throws PersistenceException in case of an error, e.g. communicating with the external storage
    * @deprecated since 9.3 This is to be removed and replaced by {@link #publishEntries(Predicate, boolean, boolean)}
    */
   @Deprecated
   default void process(KeyFilter<? super K> filter, CacheLoaderTask<K, V> task, Executor executor, boolean fetchValue, boolean fetchMetadata) {
      throw new UnsupportedOperationException("Should call processEntries!");
   }

   /**
    * Returns the number of elements in the store.
    *
    * @throws PersistenceException in case of an error, e.g. communicating with the external storage
    */
   int size();

   /**
    * Publishes all the keys from this store. The given publisher can be used by as many
    * {@link org.reactivestreams.Subscriber}s as desired. Keys are not retrieved until a given Subscriber requests
    * them from the {@link org.reactivestreams.Subscription}.
    * <p>
    * Stores will return only non expired keys
    * @implSpec
    * The default implementation just invokes {@link #publishEntries(Predicate, boolean, boolean)} passing along the
    * provided filter and {@code false} for fetchValue and {@code true} for fetchMetadata and retrieves
    * the key from the {@link MarshalledEntry}.
    * @param filter a filter - null is treated as allowing all entries
    * @return a publisher that will provide the keys from the store
    */
   default Publisher<K> publishKeys(Predicate<? super K> filter) {
      return Flowable.fromPublisher(publishEntries(filter, false, true)).map(MarshalledEntry::getKey);
   }

   /**
    * Publishes all entries from this store.  The given publisher can be used by as many
    * {@link org.reactivestreams.Subscriber}s as desired. Entries are not retrieved until a given Subscriber requests
    * them from the {@link org.reactivestreams.Subscription}.
    * <p>
    * If <b>fetchMetadata</b> is true this store must guarantee to not return any expired entries.
    * @implSpec
    * The default implementation falls back to invoking the
    * {@link #process(KeyFilter, CacheLoaderTask, Executor, boolean, boolean)} method using a
    * {@link Executors#newSingleThreadExecutor()} that is created per subscriber and is shut down when the publisher
    * completes.
    * @param filter a filter - null is treated as allowing all entries
    * @param fetchValue    whether or not to fetch the value from the persistent store. E.g. if the iteration is
    *                      intended only over the key set, no point fetching the values from the persistent store as
    *                      well
    * @param fetchMetadata whether or not to fetch the metadata from the persistent store. E.g. if the iteration is
    *                      intended only ove the key set, then no point fetching the metadata from the persistent store
    *                      as well
    * @return a publisher that will provide the entries from the store
    */
   default Publisher<MarshalledEntry<K, V>> publishEntries(Predicate<? super K> filter, boolean fetchValue,
         boolean fetchMetadata) {
      Callable<KeyValuePair<PersistenceManagerCloseableSupplier<K, V>, ExecutorService>> callable = () -> {
         ExecutorService executorService = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            t.setName("Infinispan-process-based-publish-entries");
            return t;
         });
         return new KeyValuePair<>(new PersistenceManagerCloseableSupplier<>(executorService,
               this, filter, fetchValue, fetchMetadata, 10, TimeUnit.SECONDS, 2048), executorService);
      };
      return Flowable.using(callable,
            kvp -> Flowable.fromIterable(() -> new CloseableSuppliedIterator<>(kvp.getKey())),
            kvp -> {
               kvp.getKey().close();
               kvp.getValue().shutdown();
            }
      );
   }

   /**
    * Offers a callback to be invoked for parallel iteration over the entries in an external store. Implementors should
    * be thread safe.
    * @deprecated since 9.3 The process method is deprecated and thus this class shouldn't be in use any more
    */
   @ThreadSafe
   @Deprecated
   interface CacheLoaderTask<K, V> {

      /**
       * @param marshalledEntry an iterated entry. Note that {@link org.infinispan.marshall.core.MarshalledEntry#getValue()}
       *                        might be null if the fetchValue parameter passed to {@link AdvancedCacheLoader#process(KeyFilter,
       *                        org.infinispan.persistence.spi.AdvancedCacheLoader.CacheLoaderTask,
       *                        java.util.concurrent.Executor, boolean, boolean)} is false.
       * @param taskContext     allows the implementors to decide when to stop the iteration by invoking {@link
       *                        org.infinispan.persistence.spi.AdvancedCacheLoader.TaskContext#stop()}
       */
      void processEntry(MarshalledEntry<K, V> marshalledEntry, TaskContext taskContext) throws InterruptedException;
   }

   /**
    * Used during the parallel iteration in order to offer the {@link CacheLoaderTask} a way of canceling the entry
    * iteration. Should be thread safe.
    * @deprecated since 9.3 The process method is no longer suggested and thus this class shouldn't be in use any more
    */
   @ThreadSafe
   @Deprecated
   interface TaskContext {
      /**
       * Invoked from within the CacheLoaderTask, in order to signal the AdvancedCacheLoader implementation that
       * iteration should be stopped early (before iteration is finished).
       */
      void stop();

      /**
       * Verifies if the the TaskContext is marked as stopped.
       */
      boolean isStopped();
   }
}
