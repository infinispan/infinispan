package org.infinispan.notifications.cachelistener.event;

/**
 * This event subtype is passed in to any method annotated with {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryActivated}.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheEntryActivatedEvent<K, V> extends CacheEntryEvent<K, V> {
   /**
    * Retrieves the value of the entry being activated.
    *
    * @return the value of the activated entry
    */
   V getValue();
}
