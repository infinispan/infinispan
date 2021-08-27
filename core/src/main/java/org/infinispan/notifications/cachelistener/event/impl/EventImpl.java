package org.infinispan.notifications.cachelistener.event.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import org.infinispan.Cache;
import org.infinispan.commons.util.Util;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.metadata.Metadata;
import org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryActivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryExpiredEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryInvalidatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryLoadedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryModifiedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryPassivatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryVisitedEvent;
import org.infinispan.notifications.cachelistener.event.DataRehashedEvent;
import org.infinispan.notifications.cachelistener.event.PartitionStatusChangedEvent;
import org.infinispan.notifications.cachelistener.event.PersistenceAvailabilityChangedEvent;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.notifications.cachelistener.event.TransactionCompletedEvent;
import org.infinispan.notifications.cachelistener.event.TransactionRegisteredEvent;
import org.infinispan.partitionhandling.AvailabilityMode;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;

import net.jcip.annotations.NotThreadSafe;

/**
 * Basic implementation of an event that covers all event types.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@NotThreadSafe
public class EventImpl<K, V> implements CacheEntryActivatedEvent, CacheEntryCreatedEvent, CacheEntriesEvictedEvent, CacheEntryLoadedEvent, CacheEntryModifiedEvent,
                                        CacheEntryPassivatedEvent, CacheEntryRemovedEvent, CacheEntryVisitedEvent, TransactionCompletedEvent, TransactionRegisteredEvent,
                                        CacheEntryInvalidatedEvent, DataRehashedEvent, TopologyChangedEvent, CacheEntryExpiredEvent, PartitionStatusChangedEvent,
                                        PersistenceAvailabilityChangedEvent, Cloneable {
   private boolean pre = false; // by default events are after the fact
   private transient Cache<K, V> cache;
   private K key;
   private Object source;
   private Metadata metadata;
   private Metadata oldMetadata;
   private boolean originLocal = true; // by default events all originate locally
   private boolean transactionSuccessful;
   private Type type;
   private V value;
   private V newValue;
   private V oldValue;
   private ConsistentHash readConsistentHashAtStart, writeConsistentHashAtStart, readConsistentHashAtEnd, writeConsistentHashAtEnd, unionConsistentHash;
   private int newTopologyId;
   private Map<? extends K, ? extends V> entries;
   private boolean created;
   private boolean commandRetried;
   private boolean isCurrentState;
   private AvailabilityMode mode;
   private boolean available;

   public EventImpl() {
   }

   public static <K, V> EventImpl<K, V> createEvent(Cache<K, V> cache, Type type) {
      EventImpl<K, V> e = new EventImpl<>();
      e.cache = cache;
      e.type = type;
      return e;
   }

   @Override
   public Type getType() {
      return type;
   }

   @Override
   public boolean isPre() {
      return pre;
   }

   @Override
   public Cache<K, V> getCache() {
      return cache;
   }

   @Override
   @SuppressWarnings("unchecked")
   public K getKey() {
      return key;
   }

   @Override
   public GlobalTransaction getGlobalTransaction() {
      if (this.source instanceof GlobalTransaction)
         return (GlobalTransaction) this.source;

      return null;
   }

   @Override
   public Object getSource() {
      return source;
   }

   @Override
   public boolean isOriginLocal() {
      return originLocal;
   }

   @Override
   public boolean isTransactionSuccessful() {
      return transactionSuccessful;
   }

   // ------------------------------ setters -----------------------------

   public void setPre(boolean pre) {
      this.pre = pre;
   }

   public void setKey(K key) {
      this.key = key;
   }

   /**
    * @deprecated Since 12.0, will be removed in 15.0
    */
   @Deprecated
   public void setTransactionId(GlobalTransaction transaction) {
      setSource(transaction);
   }

   /**
    * @param source An identifier of the transaction or cache invocation that triggered the event.
    *               In a transactional cache, it is the same as {@link #getGlobalTransaction()}.
    *               In a non-transactional cache, it is an internal object that identifies the cache invocation.
    */
   public void setSource(Object source) {
      this.source = source;
   }

   public void setOriginLocal(boolean originLocal) {
      this.originLocal = originLocal;
   }

   public void setTransactionSuccessful(boolean transactionSuccessful) {
      this.transactionSuccessful = transactionSuccessful;
   }

   public void setReadConsistentHashAtStart(ConsistentHash readConsistentHashAtStart) {
      this.readConsistentHashAtStart = readConsistentHashAtStart;
   }

   public void setWriteConsistentHashAtStart(ConsistentHash writeConsistentHashAtStart) {
      this.writeConsistentHashAtStart = writeConsistentHashAtStart;
   }

   public void setReadConsistentHashAtEnd(ConsistentHash readConsistentHashAtEnd) {
      this.readConsistentHashAtEnd = readConsistentHashAtEnd;
   }

   public void setWriteConsistentHashAtEnd(ConsistentHash writeConsistentHashAtEnd) {
      this.writeConsistentHashAtEnd = writeConsistentHashAtEnd;
   }

   public void setUnionConsistentHash(ConsistentHash unionConsistentHash) {
      this.unionConsistentHash = unionConsistentHash;
   }

   public void setNewTopologyId(int newTopologyId) {
      this.newTopologyId = newTopologyId;
   }

   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public boolean isCurrentState() {
      return isCurrentState;
   }

   public void setCurrentState(boolean currentState) {
      isCurrentState = currentState;
   }

   public void setOldMetadata(Metadata metadata) {
      this.oldMetadata = metadata;
   }

   public Metadata getOldMetadata() {
      return oldMetadata;
   }

   @Override
   @SuppressWarnings("unchecked")
   public V getValue() {
      return value;
   }

   public void setCommandRetried(boolean commandRetried) {
      this.commandRetried = commandRetried;
   }

   @Override
   public boolean isCommandRetried() {
      return commandRetried;
   }

   @Override
   public boolean isCreated() {
      return created;
   }

   public V getNewValue() {
      return newValue;
   }

   @Override
   public V getOldValue() {
      return oldValue;
   }

   public void setValue(V value) {
      this.value = value;
      this.newValue = value;
   }

   public void setEntries(Map<? extends K, ? extends V> entries) {
      this.entries = entries;
   }

   public void setCreated(boolean created) {
      this.created = created;
   }

   public void setNewValue(V newValue) {
      this.newValue = newValue;
   }

   public void setOldValue(V oldValue) {
      this.oldValue = oldValue;
   }

   public boolean isAvailable() {
      return available;
   }

   public void setAvailable(boolean available) {
      this.available = available;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      EventImpl<?, ?> event = (EventImpl<?, ?>) o;

      if (originLocal != event.originLocal) return false;
      if (pre != event.pre) return false;
      if (transactionSuccessful != event.transactionSuccessful) return false;
      if (cache != null ? !cache.equals(event.cache) : event.cache != null) return false;
      if (key != null ? !key.equals(event.key) : event.key != null) return false;
      if (source != null ? !source.equals(event.source) : event.source != null) return false;
      if (type != event.type) return false;
      if (value != null ? !value.equals(event.value) : event.value != null) return false;
      if (!Util.safeEquals(readConsistentHashAtStart, event.readConsistentHashAtStart)) return false;
      if (!Util.safeEquals(writeConsistentHashAtStart, event.writeConsistentHashAtStart)) return false;
      if (!Util.safeEquals(readConsistentHashAtEnd, event.readConsistentHashAtEnd)) return false;
      if (!Util.safeEquals(writeConsistentHashAtEnd, event.writeConsistentHashAtEnd)) return false;
      if (!Util.safeEquals(unionConsistentHash, event.unionConsistentHash)) return false;
      if (newTopologyId != event.newTopologyId) return false;
      if (created != event.created) return false;
      if (isCurrentState != event.isCurrentState) return false;
      if (oldValue != null ? !oldValue.equals(event.oldValue) : event.oldValue != null) return false;
      if (newValue != null ? !newValue.equals(event.newValue) : event.newValue != null) return false;

      return available == event.available;
   }

   @Override
   public int hashCode() {
      int result = (pre ? 1 : 0);
      result = 31 * result + (cache != null ? cache.hashCode() : 0);
      result = 31 * result + (key != null ? key.hashCode() : 0);
      result = 31 * result + (source != null ? source.hashCode() : 0);
      result = 31 * result + (originLocal ? 1 : 0);
      result = 31 * result + (transactionSuccessful ? 1 : 0);
      result = 31 * result + (type != null ? type.hashCode() : 0);
      result = 31 * result + (value != null ? value.hashCode() : 0);
      result = 31 * result + (readConsistentHashAtStart != null ? readConsistentHashAtStart.hashCode() : 0);
      result = 31 * result + (writeConsistentHashAtStart != null ? writeConsistentHashAtStart.hashCode() : 0);
      result = 31 * result + (readConsistentHashAtEnd != null ? readConsistentHashAtEnd.hashCode() : 0);
      result = 31 * result + (writeConsistentHashAtEnd != null ? writeConsistentHashAtEnd.hashCode() : 0);
      result = 31 * result + (unionConsistentHash != null ? unionConsistentHash.hashCode() : 0);
      result = 31 * result + newTopologyId;
      result = 31 * result + (created ? 1 : 0) + (isCurrentState ? 2 : 0);
      result = 31 * result + (oldValue != null ? oldValue.hashCode() : 0);
      result = 31 * result + (newValue != null ? newValue.hashCode() : 0);
      result = 31 * result + (available ? 1 : 0);
      return result;
   }

   @Override
   public String toString() {
      if (type == Type.TOPOLOGY_CHANGED || type == Type.DATA_REHASHED)
         return "EventImpl{" +
                "type=" + type +
                ", pre=" + pre +
                ", cache=" + cache +
                ", readConsistentHashAtStart=" + readConsistentHashAtStart +
                ", writeConsistentHashAtStart=" + writeConsistentHashAtStart +
                ", readConsistentHashAtEnd=" + readConsistentHashAtEnd +
                ", writeConsistentHashAtEnd=" + writeConsistentHashAtEnd +
                ", unionConsistentHash=" + unionConsistentHash +
                ", newTopologyId=" + newTopologyId +
                '}';
      return "EventImpl{" +
             "type=" + type +
             ", pre=" + pre +
             ", cache=" + cache +
             ", key=" + key +
             ", value=" + value +
             ", newValue=" + newValue +
             ", oldValue=" + oldValue +
             ", source=" + source +
             ", originLocal=" + originLocal +
             ", transactionSuccessful=" + transactionSuccessful +
             ", entries=" + entries +
             ", created=" + created +
             ", isCurrentState=" + isCurrentState +
             ", available=" + available +
             '}';
   }

   @Override
   public Collection<Address> getMembersAtStart() {
      return readConsistentHashAtStart != null ? readConsistentHashAtStart.getMembers() : Collections.<Address>emptySet();
   }

   @Override
   public Collection<Address> getMembersAtEnd() {
      return readConsistentHashAtEnd != null ? readConsistentHashAtEnd.getMembers() : Collections.<Address>emptySet();
   }

   @Override
   public ConsistentHash getConsistentHashAtStart() {
      return readConsistentHashAtStart;
   }

   @Override
   public ConsistentHash getConsistentHashAtEnd() {
      return writeConsistentHashAtEnd;
   }

   @Override
   public ConsistentHash getReadConsistentHashAtStart() {
      return readConsistentHashAtStart;
   }

   @Override
   public ConsistentHash getWriteConsistentHashAtStart() {
      return writeConsistentHashAtStart;
   }

   @Override
   public ConsistentHash getReadConsistentHashAtEnd() {
      return readConsistentHashAtEnd;
   }

   @Override
   public ConsistentHash getWriteConsistentHashAtEnd() {
      return writeConsistentHashAtEnd;
   }

   @Override
   public ConsistentHash getUnionConsistentHash() {
      return unionConsistentHash;
   }

   @Override
   public int getNewTopologyId() {
      return newTopologyId;
   }

   @Override
   public AvailabilityMode getAvailabilityMode() {
      return mode;
   }

   public void setAvailabilityMode(AvailabilityMode mode) {
      this.mode = mode;
   }

   @Override
   public Map<? extends K, ? extends V> getEntries() {
      return entries;
   }

   @Override
   public EventImpl<K, V> clone() {
      try {
         return (EventImpl<K, V>) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should never happen!", e);
      }
   }
}
