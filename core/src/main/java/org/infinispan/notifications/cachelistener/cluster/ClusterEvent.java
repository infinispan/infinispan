package org.infinispan.notifications.cachelistener.cluster;

import java.util.Objects;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.marshall.protostream.impl.MarshallableObject;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.impl.EventImpl;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.transaction.xa.GlobalTransaction;

/**
 * This is an event designed for use with cluster listeners solely.  This is the event that is serialized across the
 * wire when sending the event back to the node where the cluster listener is registered.  You should only create
 * a ClusterEvent through the use of the {@link ClusterEvent#fromEvent(CacheEntryEvent)} method.
 *
 * @author wburns
 * @since 7.0
 */
@ProtoTypeId(ProtoStreamTypeIds.CLUSTER_EVENT)
public class ClusterEvent<K, V> implements CacheEntryCreatedEvent<K, V>, CacheEntryRemovedEvent<K, V>,
                                           CacheEntryModifiedEvent<K, V>, CacheEntryExpiredEvent<K, V> {
   transient Cache<K, V> cache;

   private final K key;
   private final V value;
   private final V oldValue;
   private final Metadata metadata;

   @ProtoField(number = 1)
   final Type type;

   @ProtoField(number = 2)
   final GlobalTransaction transaction;

   @ProtoField(number = 3, javaType = JGroupsAddress.class)
   final Address origin;

   @ProtoField(number = 4, defaultValue = "false")
   final boolean commandRetried;

   public static <K, V> ClusterEvent<K, V> fromEvent(CacheEntryEvent<K, V> event) {
      if (event instanceof ClusterEvent) {
         return (ClusterEvent<K, V>) event;
      }
      V oldValue = null;
      Type eventType = event.getType();
      boolean commandRetried = switch (eventType) {
          case CACHE_ENTRY_REMOVED -> {
              oldValue = ((CacheEntryRemovedEvent<K, V>) event).getOldValue();
              yield ((CacheEntryRemovedEvent<K, V>) event).isCommandRetried();
          }
          case CACHE_ENTRY_CREATED -> ((CacheEntryCreatedEvent<K, V>) event).isCommandRetried();
          case CACHE_ENTRY_MODIFIED -> ((CacheEntryModifiedEvent<K, V>) event).isCommandRetried();
          case CACHE_ENTRY_EXPIRED ->
              // Expired doesn't have a retry
                false;
          default ->
                throw new IllegalArgumentException("Cluster Event can only be created from a CacheEntryRemoved, CacheEntryCreated or CacheEntryModified event!");
      };

      GlobalTransaction transaction = event.getGlobalTransaction();
      Metadata metadata = null;
      if (event instanceof EventImpl) {
         metadata = event.getMetadata();
      }

      ClusterEvent<K, V> clusterEvent = new ClusterEvent<>(event.getKey(), event.getValue(), oldValue, metadata,
            eventType, event.getCache().getCacheManager().getAddress(),
            transaction, commandRetried);
      clusterEvent.cache = event.getCache();
      return clusterEvent;
   }

   ClusterEvent(K key, V value, V oldValue, Metadata metadata, Type type, Address origin, GlobalTransaction transaction,
                boolean commandRetried) {
      this.key = key;
      this.value = value;
      this.oldValue = oldValue;
      this.metadata = metadata;
      this.type = type;
      this.origin = origin;
      this.transaction = transaction;
      this.commandRetried = commandRetried;
   }

   @ProtoFactory
   ClusterEvent(MarshallableObject<K> wrappedKey, MarshallableObject<V> wrappedValue,
                MarshallableObject<V> wrappedOldValue, MarshallableObject<Metadata> wrappedMetadata,
                GlobalTransaction transaction, JGroupsAddress origin, Type type, boolean commandRetried) {
      this(MarshallableObject.unwrap(wrappedKey), MarshallableObject.unwrap(wrappedValue),
            MarshallableObject.unwrap(wrappedOldValue), MarshallableObject.unwrap(wrappedMetadata),
            type, origin, transaction, commandRetried);
   }

   @ProtoField(number = 5, name = "key")
   MarshallableObject<K> getWrappedKey() {
      return MarshallableObject.create(key);
   }

   @ProtoField(number = 6, name = "value")
   MarshallableObject<V> getWrappedValue() {
      return MarshallableObject.create(value);
   }

   @ProtoField(number = 7, name = "oldValue")
   MarshallableObject<V> getWrappedOldValue() {
      return MarshallableObject.create(oldValue);
   }

   @ProtoField(number = 8, name = "metadata")
   MarshallableObject<Metadata> getWrappedMetadata() {
      return MarshallableObject.create(metadata);
   }

   @Override
   public V getValue() {
      return value;
   }

   @Override
   public V getNewValue() {
      return value;
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public Metadata getOldMetadata() {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isCommandRetried() {
      return commandRetried;
   }

   @Override
   public V getOldValue() {
      return oldValue;
   }

   @Override
   public boolean isCreated() {
      return type == Type.CACHE_ENTRY_CREATED;
   }

   @Override
   public K getKey() {
      return key;
   }

   @Override
   public GlobalTransaction getGlobalTransaction() {
      return transaction;
   }

   @Override
   public boolean isOriginLocal() {
      if (cache != null) {
         return cache.getCacheManager().getAddress().equals(origin);
      }
      return false;
   }

   @Override
   public Type getType() {
      return type;
   }

   @Override
   public boolean isPre() {
      // Cluster events are always sent after the value has been updated
      return false;
   }

   @Override
   public Cache<K, V> getCache() {
      return cache;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      ClusterEvent<?, ?> that = (ClusterEvent<?, ?>) o;
      return commandRetried == that.commandRetried &&
            Objects.equals(cache, that.cache) &&
            Objects.equals(key, that.key) &&
            Objects.equals(value, that.value) &&
            Objects.equals(oldValue, that.oldValue) &&
            Objects.equals(metadata, that.metadata) &&
            type == that.type &&
            Objects.equals(transaction, that.transaction) &&
            Objects.equals(origin, that.origin);
   }

   @Override
   public int hashCode() {
      return Objects.hash(cache, key, value, oldValue, metadata, type, transaction, origin, commandRetried);
   }

   @Override
   public String toString() {
      return "ClusterEvent {" +
            "type=" + type +
            ", cache=" + cache +
            ", key=" + key +
            ", value=" + value +
            ", oldValue=" + oldValue +
            ", transaction=" + transaction +
            ", retryCommand=" + commandRetried +
            ", origin=" + origin +
            '}';
   }
}
