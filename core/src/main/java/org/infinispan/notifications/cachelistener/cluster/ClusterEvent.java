package org.infinispan.notifications.cachelistener.cluster;

import org.infinispan.Cache;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.TransactionalEvent;
import org.infinispan.notifications.cachelistener.event.impl.EventImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * This is an event designed for use with cluster listeners solely.  This is the event that is serialized across the
 * wire when sending the event back to the node where the cluster listener is registered.  You should only create
 * a ClusterEvent through the use of the {@link ClusterEvent#fromEvent(CacheEntryEvent)} method.
 *
 * @author wburns
 * @since 7.0
 */
public class ClusterEvent<K, V> implements CacheEntryCreatedEvent<K, V>, CacheEntryRemovedEvent<K, V>,
                                           CacheEntryModifiedEvent<K, V> {
   transient Cache<K, V> cache;

   private final K key;
   private final V value;
   private final V oldValue;
   private final Metadata metadata;
   private final Type type;
   private final GlobalTransaction transaction;
   private final Address origin;
   private final boolean commandRetried;

   public static <K, V>ClusterEvent<K, V> fromEvent(CacheEntryEvent<K, V> event) {
      if (event instanceof ClusterEvent) {
         return (ClusterEvent)event;
      }
      V oldValue = null;
      Type eventType = event.getType();
      boolean commandRetried;
      switch (eventType) {
         case CACHE_ENTRY_REMOVED:
            oldValue = ((CacheEntryRemovedEvent<K, V>)event).getOldValue();
            commandRetried = ((CacheEntryRemovedEvent<K, V>)event).isCommandRetried();
            break;
         case CACHE_ENTRY_CREATED:
            commandRetried = ((CacheEntryCreatedEvent<K, V>)event).isCommandRetried();
            break;
         case CACHE_ENTRY_MODIFIED:
            commandRetried = ((CacheEntryModifiedEvent<K, V>)event).isCommandRetried();
            break;
         default:
            throw new IllegalArgumentException("Cluster Event can only be created from a CacheEntryRemoved, CacheEntryCreated or CacheEntryModified event!");
      }

      GlobalTransaction transaction = ((TransactionalEvent)event).getGlobalTransaction();

      Metadata metadata = null;
      if (event instanceof EventImpl) {
         metadata = ((EventImpl)event).getMetadata();
      }

      ClusterEvent<K, V> clusterEvent = new ClusterEvent<K, V>(event.getKey(), event.getValue(), oldValue, metadata,
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

   @Override
   public V getValue() {
      return value;
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
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

   public static class Externalizer extends AbstractExternalizer<ClusterEvent> {

      @Override
      public Set<Class<? extends ClusterEvent>> getTypeClasses() {
         return Util.<Class<? extends ClusterEvent>>asSet(ClusterEvent.class);
      }

      @Override
      public void writeObject(ObjectOutput output, ClusterEvent object) throws IOException {
         output.writeObject(object.key);
         output.writeObject(object.value);
         output.writeObject(object.oldValue);
         output.writeObject(object.metadata);
         output.writeObject(object.type);
         output.writeObject(object.origin);
         output.writeObject(object.transaction);
         output.writeBoolean(object.commandRetried);
      }

      @Override
      public ClusterEvent readObject(ObjectInput input) throws IOException, ClassNotFoundException {

         return new ClusterEvent(input.readObject(), input.readObject(), input.readObject(),
                                 (Metadata)input.readObject(),(Type)input.readObject(),
                                 (Address)input.readObject(), (GlobalTransaction)input.readObject(),
                                 input.readBoolean());
      }

      @Override
      public Integer getId() {
         return Ids.CLUSTER_EVENT;
      }
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ClusterEvent that = (ClusterEvent) o;

      if (commandRetried != that.commandRetried) return false;
      if (cache != null ? !cache.equals(that.cache) : that.cache != null) return false;
      if (key != null ? !key.equals(that.key) : that.key != null) return false;
      if (metadata != null ? !metadata.equals(that.metadata) : that.metadata != null) return false;
      if (oldValue != null ? !oldValue.equals(that.oldValue) : that.oldValue != null) return false;
      if (origin != null ? !origin.equals(that.origin) : that.origin != null) return false;
      if (transaction != null ? !transaction.equals(that.transaction) : that.transaction != null) return false;
      if (type != that.type) return false;
      if (value != null ? !value.equals(that.value) : that.value != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = cache != null ? cache.hashCode() : 0;
      result = 31 * result + (key != null ? key.hashCode() : 0);
      result = 31 * result + (value != null ? value.hashCode() : 0);
      result = 31 * result + (oldValue != null ? oldValue.hashCode() : 0);
      result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
      result = 31 * result + (type != null ? type.hashCode() : 0);
      result = 31 * result + (transaction != null ? transaction.hashCode() : 0);
      result = 31 * result + (origin != null ? origin.hashCode() : 0);
      result = 31 * result + (commandRetried ? 1 : 0);
      return result;
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
