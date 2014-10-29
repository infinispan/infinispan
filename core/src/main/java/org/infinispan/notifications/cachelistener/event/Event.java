package org.infinispan.notifications.cachelistener.event;

import org.infinispan.Cache;

/**
 * An interface that defines common characteristics of events
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface Event<K, V> {
   static enum Type {
      CACHE_ENTRY_ACTIVATED, CACHE_ENTRY_PASSIVATED, CACHE_ENTRY_VISITED,
      CACHE_ENTRY_LOADED, CACHE_ENTRY_EVICTED, CACHE_ENTRY_CREATED, CACHE_ENTRY_REMOVED, CACHE_ENTRY_MODIFIED,
      TRANSACTION_COMPLETED, TRANSACTION_REGISTERED, CACHE_ENTRY_INVALIDATED, DATA_REHASHED, TOPOLOGY_CHANGED,
      PARTITION_STATUS_CHANGED
   }

   /**
    * @return the type of event represented by this instance.
    */
   Type getType();

   /**
    * @return <tt>true</tt> if the notification is before the event has occurred, <tt>false</tt> if after the event has occurred.
    */
   boolean isPre();

   /**
    * @return a handle to the cache instance that generated this notification.
    */
   Cache<K, V> getCache();
}
