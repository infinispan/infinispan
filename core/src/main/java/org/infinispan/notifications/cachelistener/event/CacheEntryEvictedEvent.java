package org.infinispan.notifications.cachelistener.event;

/**
 * This event subtype is passed in to any method annotated with
 * {@link org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted}.
 *
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @deprecated Note that this interface will be removed in Infinispan 6.0
 * @see org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted
 */
@Deprecated
public interface CacheEntryEvictedEvent<K, V> extends CacheEntryEvent<K, V> {

   /**
    * Retrieves the value of the entry being evicted.
    * <p />
    * @return the value of the entry being evicted.
    */
   V getValue();

}