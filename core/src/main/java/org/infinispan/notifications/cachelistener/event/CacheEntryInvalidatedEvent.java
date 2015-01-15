package org.infinispan.notifications.cachelistener.event;

/**
 * Notifies a listener of an invalidation event.
 * <p>
 * Eviction has no notion of pre/post event since 4.2.0.ALPHA4.  This event is only
 * raised once after the eviction has occurred with the pre event flag being false.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheEntryInvalidatedEvent<K, V> extends CacheEntryEvent<K, V> {
   /**
    * Retrieves the value of the entry being invalidated.
    *
    * @return the value of the invalidated entry
    */
   V getValue();
}
