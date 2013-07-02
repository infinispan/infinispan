package org.infinispan.notifications.cachelistener.event;

import java.util.Map;

/**
 * This event subtype is passed in to any method annotated with
 * {@link org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted}.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 5.0
 */
public interface CacheEntriesEvictedEvent<K, V> extends Event<K, V> {

   /**
    * Retrieves entries being evicted.
    *
    * @return A map containing the key/value pairs of the
    *         cache entries being evicted.
    */
   Map<K, V> getEntries();

}
