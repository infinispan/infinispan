package org.infinispan.persistence.spi;

import net.jcip.annotations.ThreadSafe;

import java.util.concurrent.Executor;

/**
 * Defines advanced functionality for persisting data to an external storage.
 *
 * @author Mircea Markus
 * @since 6.0
 */
@ThreadSafe
public interface AdvancedCacheWriter<K, V> extends CacheWriter<K, V> {

   /**
    * Removes all the data from the storage.
    *
    * @throws PersistenceException in case of an error, e.g. communicating with the external storage
    */
   void clear();

   /**
    * Using the thread in the pool, removed all the expired data from the persistence storage. For each removed entry,
    * the supplied listener is invoked.
    *
    * @throws PersistenceException in case of an error, e.g. communicating with the external storage
    */
   void purge(Executor threadPool, PurgeListener<? super K> listener);

   /**
    * Callback to be notified when an entry is removed by the {@link #purge(java.util.concurrent.Executor,
    * org.infinispan.persistence.spi.AdvancedCacheWriter.PurgeListener)} method.
    */
   public interface PurgeListener<K> {

      /**
       * Optional. If possible, {@link AdvancedCacheWriter} implementors should invoke this method for every entry that
       * is purged from the store. One of the side effects of not implementing this method is that listeners do not
       * receive {@link org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted} for the entries that
       * are removed from the persistent store directly.
       */
      void entryPurged(K key);
   }
}
