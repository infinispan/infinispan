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
public interface AdvancedCacheWriter<K,V> extends CacheWriter<K,V> {

   /**
    * Removes all the data from the storage.
    */
   void clear();

   /**
    * Using the thread in the pool, removed all the expired data from the persistence storage. For each removed entry, the
    * supplied listener is invoked.
    */
   void purge(Executor threadPool, PurgeListener listener);

   public interface PurgeListener<K> {
      void entryPurged(K key);
   }
}
