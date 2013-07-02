package org.infinispan.notifications.cachelistener.event;

/**
 * This event subtype is passed in to any method annotated with
 * {@link org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated}.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface CacheEntryCreatedEvent<K, V> extends CacheEntryEvent<K, V> {

   /**
    * Retrieves the value of the entry being created.
    *
    * @return null if {@link #isPre()} is true, or the value being created
    * if {@link #isPre()} is false.
    */
   V getValue();

}
