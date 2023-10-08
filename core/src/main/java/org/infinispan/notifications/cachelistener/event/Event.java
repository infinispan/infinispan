package org.infinispan.notifications.cachelistener.event;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * An interface that defines common characteristics of events
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface Event<K, V> {

   @Proto
   @ProtoTypeId(ProtoStreamTypeIds.CLUSTER_EVENT_TYPE)
   enum Type {
      CACHE_ENTRY_ACTIVATED, CACHE_ENTRY_PASSIVATED, CACHE_ENTRY_VISITED,
      CACHE_ENTRY_LOADED, CACHE_ENTRY_EVICTED, CACHE_ENTRY_CREATED, CACHE_ENTRY_REMOVED, CACHE_ENTRY_MODIFIED,
      TRANSACTION_COMPLETED, TRANSACTION_REGISTERED, CACHE_ENTRY_INVALIDATED, CACHE_ENTRY_EXPIRED, DATA_REHASHED,
      TOPOLOGY_CHANGED, PARTITION_STATUS_CHANGED, PERSISTENCE_AVAILABILITY_CHANGED;
   }

   /**
    * @return the type of event represented by this instance.
    */
   Type getType();

   /**
    * @return <code>true</code> if the notification is before the event has occurred, <code>false</code> if after the event has occurred.
    */
   boolean isPre();

   /**
    * @return a handle to the cache instance that generated this notification.
    */
   Cache<K, V> getCache();
}
