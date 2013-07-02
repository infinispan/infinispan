package org.infinispan.notifications.cachelistener.event;

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
}
