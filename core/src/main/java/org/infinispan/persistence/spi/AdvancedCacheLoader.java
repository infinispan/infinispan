package org.infinispan.persistence.spi;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.filter.KeyFilter;
import org.infinispan.marshall.core.MarshalledEntry;

import java.util.concurrent.Executor;

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
    */
   void process(KeyFilter<? super K> filter, CacheLoaderTask<K, V> task, Executor executor, boolean fetchValue, boolean fetchMetadata);

   /**
    * Returns the number of elements in the store.
    *
    * @throws PersistenceException in case of an error, e.g. communicating with the external storage
    */
   int size();

   /**
    * Offers a callback to be invoked for parallel iteration over the entries in an external store. Implementors should
    * be thread safe.
    */
   @ThreadSafe
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
    */
   @ThreadSafe
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
