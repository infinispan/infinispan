package org.infinispan.notifications.cachelistener.event;

import org.infinispan.metadata.Metadata;

/**
 * A transactional event subtype that additionally expose a key as such events pertain to a specific cache entry.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheEntryEvent<K, V> extends TransactionalEvent<K, V> {
   /**
    * @return the key to the affected cache entry.
    */
   K getKey();

   /**
    * Retrieves the value of the affected cache entry
    *
    * @return the value of the cache entry
    */
   V getValue();

   /**
    * Retrieves the metadata associated with the entry.
    *
    * @return the metadata of the cache entry
    * @since 7.0
    */
   Metadata getMetadata();

}
