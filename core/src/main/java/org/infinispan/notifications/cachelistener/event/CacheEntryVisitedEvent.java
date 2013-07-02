package org.infinispan.notifications.cachelistener.event;

/**
 * This event subtype is passed in to any method annotated with {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryVisited}.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheEntryVisitedEvent<K, V> extends CacheEntryEvent<K, V> {
   /**
    * Retrieves the value of the entry being visited.
    *
    * @return the value of the visited entry
    */
   V getValue();
}
