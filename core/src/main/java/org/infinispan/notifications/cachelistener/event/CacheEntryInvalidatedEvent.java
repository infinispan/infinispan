package org.infinispan.notifications.cachelistener.event;

/**
 * Notifies a listener of an invalidation event.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheEntryInvalidatedEvent<K, V> extends CacheEntryEvent<K, V> {
   /**
    * Retrieves the value of the entry being activated.
    *
    * @return the value of the invalidated entry
    */
   V getValue();
}
