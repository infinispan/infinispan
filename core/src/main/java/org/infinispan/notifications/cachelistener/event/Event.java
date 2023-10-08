package org.infinispan.notifications.cachelistener.event;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoEnumValue;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * An interface that defines common characteristics of events
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface Event<K, V> {

   @ProtoTypeId(ProtoStreamTypeIds.CLUSTER_EVENT_TYPE)
   enum Type {
      @ProtoEnumValue(number = 1)
      CACHE_ENTRY_ACTIVATED,

      @ProtoEnumValue(number = 2)
      CACHE_ENTRY_PASSIVATED,

      @ProtoEnumValue(number = 3)
      CACHE_ENTRY_VISITED,

      @ProtoEnumValue(number = 4)
      CACHE_ENTRY_LOADED,

      @ProtoEnumValue(number = 5)
      CACHE_ENTRY_EVICTED,

      @ProtoEnumValue(number = 6)
      CACHE_ENTRY_CREATED,

      @ProtoEnumValue(number = 7)
      CACHE_ENTRY_REMOVED,

      @ProtoEnumValue(number = 8)
      CACHE_ENTRY_MODIFIED,

      @ProtoEnumValue(number = 9)
      TRANSACTION_COMPLETED,

      @ProtoEnumValue(number = 10)
      TRANSACTION_REGISTERED,

      @ProtoEnumValue(number = 11)
      CACHE_ENTRY_INVALIDATED,

      @ProtoEnumValue(number = 12)
      CACHE_ENTRY_EXPIRED,

      @ProtoEnumValue(number = 13)
      DATA_REHASHED,

      @ProtoEnumValue(number = 14)
      TOPOLOGY_CHANGED,

      @ProtoEnumValue(number = 15)
      PARTITION_STATUS_CHANGED,

      @ProtoEnumValue(number = 16)
      PERSISTENCE_AVAILABILITY_CHANGED;
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
