/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.notifications.cachelistener.event;

import net.jcip.annotations.NotThreadSafe;
import org.infinispan.Cache;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Util;

import java.util.Collection;
import java.util.Map;

/**
 * Basic implementation of an event that covers all event types.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@NotThreadSafe
public class EventImpl<K, V> implements CacheEntryActivatedEvent, CacheEntryCreatedEvent, CacheEntriesEvictedEvent, CacheEntryLoadedEvent, CacheEntryModifiedEvent,
                                        CacheEntryPassivatedEvent, CacheEntryRemovedEvent, CacheEntryVisitedEvent, TransactionCompletedEvent, TransactionRegisteredEvent,
                                  CacheEntryInvalidatedEvent, DataRehashedEvent, TopologyChangedEvent, CacheEntryEvictedEvent {
   private boolean pre = false; // by default events are after the fact
   private Cache<K, V> cache;
   private K key;
   private GlobalTransaction transaction;
   private boolean originLocal = true; // by default events all originate locally
   private boolean transactionSuccessful;
   private Type type;
   private V value;
   private Collection<Address> membersAtStart, membersAtEnd;
   private ConsistentHash consistentHashAtStart, consistentHashAtEnd;
   private long newViewId;
   private Map<Object, Object> entries;

   public EventImpl() {
   }

   public static <K, V> EventImpl<K, V> createEvent(Cache<K, V> cache, Type type) {
      EventImpl<K, V> e = new EventImpl<K,V>();
      e.cache = cache;
      e.type = type;
      return e;
   }

   public Type getType() {
      return type;
   }

   public boolean isPre() {
      return pre;
   }

   public Cache<K, V> getCache() {
      return cache;
   }

   @SuppressWarnings("unchecked")
   public K getKey() {
      if (key instanceof MarshalledValue)
         key = (K) ((MarshalledValue) key).get();
      return key;
   }

   public GlobalTransaction getGlobalTransaction() {
      return this.transaction;
   }

   public boolean isOriginLocal() {
      return originLocal;
   }

   public boolean isTransactionSuccessful() {
      return transactionSuccessful;
   }

   // ------------------------------ setters -----------------------------

   public void setPre(boolean pre) {
      this.pre = pre;
   }

   public void setCache(Cache<K, V> cache) {
      this.cache = cache;
   }

   public void setKey(K key) {
      this.key = key;
   }

   public void setTransactionId(GlobalTransaction transaction) {
      this.transaction = transaction;
   }

   public void setOriginLocal(boolean originLocal) {
      this.originLocal = originLocal;
   }

   public void setTransactionSuccessful(boolean transactionSuccessful) {
      this.transactionSuccessful = transactionSuccessful;
   }

   public void setType(Type type) {
      this.type = type;
   }

   public void setMembersAtStart(Collection<Address> membersAtStart) {
      this.membersAtStart = membersAtStart;
   }

   public void setMembersAtEnd(Collection<Address> membersAtEnd) {
      this.membersAtEnd = membersAtEnd;
   }

   public void setConsistentHashAtStart(ConsistentHash consistentHashAtStart) {
      this.consistentHashAtStart = consistentHashAtStart;
   }

   public void setConsistentHashAtEnd(ConsistentHash consistentHashAtEnd) {
      this.consistentHashAtEnd = consistentHashAtEnd;
   }

   public void setNewViewId(long newViewId) {
      this.newViewId = newViewId;
   }

   @SuppressWarnings("unchecked")
   public V getValue() {
      if (value instanceof MarshalledValue)
         value = (V) ((MarshalledValue) value).get();
      return value;
   }

   public void setValue(V value) {
      this.value = value;
   }

   public void setEntries(Map<Object, Object> entries) {
      this.entries = entries;
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
      if (transaction != null ? !transaction.equals(event.transaction) : event.transaction != null) return false;
      if (type != event.type) return false;
      if (value != null ? !value.equals(event.value) : event.value != null) return false;
      if (!Util.safeEquals(consistentHashAtStart, event.consistentHashAtStart)) return false;
      if (!Util.safeEquals(consistentHashAtEnd, event.consistentHashAtEnd)) return false;
      if (!Util.safeEquals(membersAtStart, event.membersAtStart)) return false;
      if (!Util.safeEquals(membersAtEnd, event.membersAtEnd)) return false;
      if (newViewId != event.newViewId) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = (pre ? 1 : 0);
      result = 31 * result + (cache != null ? cache.hashCode() : 0);
      result = 31 * result + (key != null ? key.hashCode() : 0);
      result = 31 * result + (transaction != null ? transaction.hashCode() : 0);
      result = 31 * result + (originLocal ? 1 : 0);
      result = 31 * result + (transactionSuccessful ? 1 : 0);
      result = 31 * result + (type != null ? type.hashCode() : 0);
      result = 31 * result + (value != null ? value.hashCode() : 0);
      result = 31 * result + (membersAtStart != null ? membersAtStart.hashCode() : 0);
      result = 31 * result + (membersAtEnd != null ? membersAtEnd.hashCode() : 0);
      result = 31 * result + (consistentHashAtStart != null ? consistentHashAtStart.hashCode() : 0);
      result = 31 * result + (consistentHashAtEnd != null ? consistentHashAtEnd.hashCode() : 0);
      result = 31 * result + ((int) newViewId);
      return result;
   }

   @Override
   public String toString() {
      return "EventImpl{" +
            "pre=" + pre +
            ", key=" + key +
            ", transaction=" + transaction +
            ", originLocal=" + originLocal +
            ", transactionSuccessful=" + transactionSuccessful +
            ", type=" + type +
            ", value=" + value +
            '}';
   }

   @Override
   public Collection<Address> getMembersAtStart() {
      return membersAtStart;
   }

   @Override
   public Collection<Address> getMembersAtEnd() {
      return membersAtEnd;
   }

   @Override
   public long getNewViewId() {
      return newViewId;
   }

   @Override
   public ConsistentHash getConsistentHashAtStart() {
      return consistentHashAtStart;
   }

   @Override
   public ConsistentHash getConsistentHashAtEnd() {
      return consistentHashAtEnd;
   }

   @Override
   public Map<K, V> getEntries() {
      return (Map<K, V>) entries;
   }
}
