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
package org.infinispan.atomic;

import org.infinispan.AdvancedCache;
import org.infinispan.batch.AutoBatchSupport;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Collection;
import java.util.Map;
import java.util.Set;


/**
 * A layer of indirection around an {@link AtomicHashMap} to provide consistency and isolation for concurrent readers
 * while writes may also be going on.  The techniques used in this implementation are very similar to the lock-free
 * reader MVCC model used in the {@link org.infinispan.container.entries.MVCCEntry} implementations for the core data
 * container, which closely follow software transactional memory approaches to dealing with concurrency.
 * <br /><br />
 * Implementations of this class are rarely created on their own; {@link AtomicHashMap#getProxy(org.infinispan.AdvancedCache, Object, boolean)}}
 * should be used to retrieve an instance of this proxy.
 * <br /><br />
 * Typically proxies are only created by the {@link AtomicMapLookup} helper, and would not be created by end-user code
 * directly.
 *
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 * @author Manik Surtani
 * @see AtomicHashMap
 * @since 4.0
 */
public class AtomicHashMapProxy<K, V> extends AutoBatchSupport implements AtomicMap<K, V> {
   private static final Log log = LogFactory.getLog(AtomicHashMapProxy.class);
   private static final boolean trace = log.isTraceEnabled();
   protected final Object deltaMapKey;
   protected final AdvancedCache<Object, AtomicMap<K, V>> cache;
   protected final AdvancedCache<Object, AtomicMap<K, V>> cacheForWriting;
   protected volatile boolean startedReadingMap = false;
   protected TransactionTable transactionTable;
   protected TransactionManager transactionManager;

   AtomicHashMapProxy(AdvancedCache<?, ?> cache, Object deltaMapKey) {
      this.cache = (AdvancedCache<Object, AtomicMap<K, V>>) cache;
      Flag[] writeFlags = new Flag[]{Flag.DELTA_WRITE};
      this.cacheForWriting = this.cache.withFlags(writeFlags);
      this.deltaMapKey = deltaMapKey;
      this.batchContainer = cache.getBatchContainer();
      transactionTable = cache.getComponentRegistry().getComponent(TransactionTable.class);
      transactionManager = cache.getTransactionManager();
   }

   // internal helper, reduces lots of casts.
   @SuppressWarnings("unchecked")
   protected AtomicHashMap<K, V> toMap(Object object) {
      Object map = (object instanceof MarshalledValue) ? ((MarshalledValue) object).get() : object;
      return (AtomicHashMap<K, V>) map;
   }

   protected AtomicHashMap<K, V> getDeltaMapForRead() {
      AtomicHashMap<K, V> ahm = toMap(cache.get(deltaMapKey));
      if (ahm != null && !startedReadingMap) startedReadingMap = true;
      assertValid(ahm);
      return ahm;
   }

   /**
    * Looks up the CacheEntry stored in transactional context corresponding to this AtomicMap.  If this AtomicMap
    * has yet to be touched by the current transaction, this method will return a null.
    *
    * @return
    */
   protected CacheEntry lookupEntryFromCurrentTransaction() {
      // Prior to 5.1, this used to happen by grabbing any InvocationContext in ThreadLocal.  Since ThreadLocals
      // can no longer be relied upon in 5.1, we need to grab the TransactionTable and check if an ongoing
      // transaction exists, peeking into transactional state instead.
      try {
         Transaction tx = transactionManager.getTransaction();
         LocalTransaction localTransaction = tx == null ? null : transactionTable.getLocalTransaction(tx);

         // The stored localTransaction could be null, if this is the first call in a transaction.  In which case
         // we know that there is no transactional state to refer to - i.e., no entries have been looked up as yet.
         return localTransaction == null ? null : localTransaction.lookupEntry(deltaMapKey);
      } catch (SystemException e) {
         return null;
      }
   }

   @SuppressWarnings("unchecked")
   protected AtomicHashMap<K, V> getDeltaMapForWrite() {
      CacheEntry lookedUpEntry = lookupEntryFromCurrentTransaction();

      boolean lockedAndCopied = lookedUpEntry != null && lookedUpEntry.isChanged() &&
            toMap(lookedUpEntry.getValue()).copied;

      if (lockedAndCopied) {
         return getDeltaMapForRead();
      } else {
         AdvancedCache<Object, AtomicMap<K, V>> cacheForRead = cache;
         if (cache.getCacheConfiguration().transaction().lockingMode() == LockingMode.PESSIMISTIC) {
            cacheForRead = cache.withFlags(Flag.FORCE_WRITE_LOCK);
         }
         // acquire WL
         AtomicHashMap<K, V> map = toMap(cacheForRead.get(deltaMapKey));
         if (map != null && !startedReadingMap) startedReadingMap = true;
         assertValid(map);

         // copy for write
         AtomicHashMap<K, V> copy = map == null ? new AtomicHashMap<K, V>(true) : map.copy();
         copy.initForWriting();
         cacheForWriting.put(deltaMapKey, copy);
         return copy;
      }
   }

   // readers

   protected void assertValid(AtomicHashMap<?, ?> map) {
      if (startedReadingMap && (map == null || map.removed))
         throw new IllegalStateException("AtomicMap stored under key " + deltaMapKey + " has been concurrently removed!");
   }

   @Override
   public Set<K> keySet() {
      AtomicHashMap<K, V> map = getDeltaMapForRead();
      return map == null ? InfinispanCollections.<K>emptySet() : map.keySet();
   }

   @Override
   public Collection<V> values() {
      AtomicHashMap<K, V> map = getDeltaMapForRead();
      return map == null ? InfinispanCollections.<V>emptySet() : map.values();
   }

   @Override
   public Set<Entry<K, V>> entrySet() {
      AtomicHashMap<K, V> map = getDeltaMapForRead();
      return map == null ? InfinispanCollections.<Entry<K, V>>emptySet() :
            map.entrySet();
   }

   @Override
   public int size() {
      AtomicHashMap<K, V> map = getDeltaMapForRead();
      return map == null ? 0 : map.size();
   }

   @Override
   public boolean isEmpty() {
      AtomicHashMap<K, V> map = getDeltaMapForRead();
      return map == null || map.isEmpty();
   }

   @Override
   public boolean containsKey(Object key) {
      AtomicHashMap<K, V> map = getDeltaMapForRead();
      return map != null && map.containsKey(key);
   }

   @Override
   public boolean containsValue(Object value) {
      AtomicHashMap<K, V> map = getDeltaMapForRead();
      return map != null && map.containsValue(value);
   }

   @Override
   public V get(Object key) {
      AtomicHashMap<K, V> map = getDeltaMapForRead();
      return map == null ? null : map.get(key);
   }

   //writers      
   @Override
   public V put(K key, V value) {
      AtomicHashMap<K, V> deltaMapForWrite;
      try {
         startAtomic();
         deltaMapForWrite = getDeltaMapForWrite();
         return deltaMapForWrite.put(key, value);
      } finally {
         endAtomic();
      }
   }

   @Override
   public V remove(Object key) {
      AtomicHashMap<K, V> deltaMapForWrite;
      try {
         startAtomic();
         deltaMapForWrite = getDeltaMapForWrite();
         return deltaMapForWrite.remove(key);
      } finally {
         endAtomic();
      }
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m) {
      AtomicHashMap<K, V> deltaMapForWrite;
      try {
         startAtomic();
         deltaMapForWrite = getDeltaMapForWrite();
         deltaMapForWrite.putAll(m);
      } finally {
         endAtomic();
      }
   }

   @Override
   public void clear() {
      AtomicHashMap<K, V> deltaMapForWrite;
      try {
         startAtomic();
         deltaMapForWrite = getDeltaMapForWrite();
         deltaMapForWrite.clear();
      } finally {
         endAtomic();
      }
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder("AtomicHashMapProxy{deltaMapKey=");
      sb.append(deltaMapKey);
      sb.append("}");
      return sb.toString();
   }
}
