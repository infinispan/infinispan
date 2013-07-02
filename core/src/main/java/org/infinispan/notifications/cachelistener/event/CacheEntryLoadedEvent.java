package org.infinispan.notifications.cachelistener.event;

/**
 * This event subtype is passed in to any method annotated with {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryLoaded}.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheEntryLoadedEvent<K, V> extends CacheEntryEvent<K, V> {
   /**
    * Retrieves the value of the entry being loaded.
    *
    * @return the value of the loaded entry
    */
   V getValue();
}
