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
import org.infinispan.context.FlagContainer;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.transaction.LocalTransaction;
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
 * Implementations of this class are rarely created on their own; {@link AtomicHashMap#getProxy(org.infinispan.AdvancedCache, Object, boolean, org.infinispan.context.FlagContainer)}}
 * should be used to retrieve an instance of this proxy.
 * <br /><br />
 * Typically proxies are only created by the {@link AtomicMapLookup} helper, and would not be created by end-user code
 * directly.
 *
 * @author Manik Surtani
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
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
   protected final FlagContainer flagContainer;
   protected TransactionTable transactionTable;
   protected TransactionManager transactionManager;

   AtomicHashMapProxy(AdvancedCache<Object, AtomicMap<K, V>> cache, Object deltaMapKey) {
      this(cache, deltaMapKey, null);
   }

   AtomicHashMapProxy(AdvancedCache<?, ?> cache, Object deltaMapKey, FlagContainer flagContainer) {
      this.cache = (AdvancedCache<Object, AtomicMap<K, V>>) cache;

      Flag[] flags = new Flag[2];
      flags[0] = Flag.SKIP_REMOTE_LOOKUP;
      // When passivation is enabled, cache loader needs to attempt to load
      // the previous value in order to merge it if necessary, so mark atomic
      // hash map writes as delta writes
      if (cache.getCacheConfiguration().loaders().passivation() || cache.getCacheConfiguration().eviction().strategy().isEnabled())
         flags[1] = Flag.DELTA_WRITE;
      else
         flags[1] = Flag.SKIP_CACHE_LOAD;

      this.cacheForWriting = this.cache.withFlags(flags);
      this.deltaMapKey = deltaMapKey;
      this.batchContainer = cache.getBatchContainer();
      this.flagContainer = flagContainer;
      transactionTable = cache.getComponentRegistry().getComponent(TransactionTable.class);
      transactionManager = cache.getTransactionManager();
   }

   @SuppressWarnings("unchecked")
   protected AtomicHashMap<K, V> toMap(Object object) {
      Object map = (object instanceof MarshalledValue) ? ((MarshalledValue) object).get() : object;
      return (AtomicHashMap<K, V>) map;
   }

   // internal helper, reduces lots of casts.
   protected AtomicHashMap<K, V> getDeltaMapForRead() {
      AtomicHashMap<K, V> ahm = toMap(cache.get(deltaMapKey));
      if (ahm != null && !startedReadingMap) startedReadingMap = true;
      assertValid(ahm);
      return ahm;
   }

   /**
    * Looks up the CacheEntry stored in transactional context corresponding to this AtomicMap.  If this AtomicMap
    * has yet to be touched by the current transaction, this method will return a null.
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
         // acquire WL
         boolean suppressLocks = flagContainer != null && flagContainer.hasFlag(Flag.SKIP_LOCKING);
         if (!suppressLocks && flagContainer != null) flagContainer.setFlags(Flag.FORCE_WRITE_LOCK);

         if (trace) {
            if (suppressLocks)
               log.trace("Skip locking flag used.  Skipping locking.");
            else
               log.trace("Forcing write lock even for reads");
         }

         // reinstate the flag
         if (suppressLocks) flagContainer.setFlags(Flag.SKIP_LOCKING);
         
         AtomicHashMap<K, V> map = getDeltaMapForRead();
         
         // copy for write
         AtomicHashMap<K, V> copy = map == null ? new AtomicHashMap<K, V>(true) : map.copy();          
         copy.initForWriting();                 
         cacheForWriting.put(deltaMapKey, copy);
         return copy;
      }
   }

   // readers

   protected void assertValid(AtomicHashMap<?, ?> map) {
      if (startedReadingMap && (map == null || map.removed)) throw new IllegalStateException("AtomicMap stored under key " + deltaMapKey + " has been concurrently removed!");
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
      return map == null ? InfinispanCollections.<Entry<K,V>>emptySet() :
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
      }
      finally {         
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
      }
      finally {         
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
      }
      finally {        
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
