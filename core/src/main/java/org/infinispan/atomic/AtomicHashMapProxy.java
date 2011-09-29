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

import org.infinispan.Cache;
import org.infinispan.batch.AutoBatchSupport;
import org.infinispan.batch.BatchContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * A layer of indirection around an {@link AtomicHashMap} to provide consistency and isolation for concurrent readers
 * while writes may also be going on.  The techniques used in this implementation are very similar to the lock-free
 * reader MVCC model used in the {@link org.infinispan.container.entries.MVCCEntry} implementations for the core data
 * container, which closely follow software transactional memory approaches to dealing with concurrency.
 * <br /><br />
 * Implementations of this class are rarely created on their own; {@link AtomicHashMap#getProxy(org.infinispan.Cache, Object, org.infinispan.batch.BatchContainer, org.infinispan.context.InvocationContextContainer)}
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
   Object deltaMapKey;
   Cache cache;
   InvocationContextContainer icc;
   volatile boolean startedReadingMap = false;

   AtomicHashMapProxy(Cache<?, ?> cache, Object deltaMapKey, BatchContainer batchContainer, InvocationContextContainer icc) {
      this.cache = cache;
      this.deltaMapKey = deltaMapKey;
      this.batchContainer = batchContainer;
      this.icc = icc;
   }

   @SuppressWarnings("unchecked")
   private AtomicHashMap<K, V> toMap(Object object) {
      Object map = (object instanceof MarshalledValue) ? ((MarshalledValue) object).get() : object;
      return (AtomicHashMap<K, V>) map;
   }

   // internal helper, reduces lots of casts.
   private AtomicHashMap<K, V> getDeltaMapForRead() {
      AtomicHashMap<K, V> ahm = toMap(cache.get(deltaMapKey));
      if (ahm != null && !startedReadingMap) startedReadingMap = true;
      assertValid(ahm);
      return ahm;
   }

   @SuppressWarnings("unchecked")
   private AtomicHashMap<K, V> getDeltaMapForWrite(InvocationContext ctx) {
      CacheEntry lookedUpEntry = ctx.lookupEntry(deltaMapKey);
      boolean lockedAndCopied = lookedUpEntry != null && lookedUpEntry.isChanged() &&
            toMap(lookedUpEntry.getValue()).copied;

      if (lockedAndCopied) {
         return getDeltaMapForRead();
      } else {
         // acquire WL
         boolean suppressLocks = ctx.hasFlag(Flag.SKIP_LOCKING);
         if (!suppressLocks) ctx.setFlags(Flag.FORCE_WRITE_LOCK);

         if (trace) {
            if (suppressLocks)
               log.trace("Skip locking flag used.  Skipping locking.");
            else
               log.trace("Forcing write lock even for reads");
         }

         AtomicHashMap<K, V> map = getDeltaMapForRead();
         // copy for write
         AtomicHashMap<K, V> copy = map == null ? new AtomicHashMap<K, V>(true) : map.copyForWrite();
         copy.initForWriting();
         // reinstate the flag
         if (suppressLocks) ctx.setFlags(Flag.SKIP_LOCKING);
         cache.put(deltaMapKey, copy);
         return copy;
      }
   }

   // readers

   private void assertValid(AtomicHashMap<?, ?> map) {
      if (startedReadingMap && (map == null || map.removed)) throw new IllegalStateException("AtomicMap stored under key " + deltaMapKey + " has been concurrently removed!");
   }

   public Set<K> keySet() {
      AtomicHashMap<K, V> map = getDeltaMapForRead();
      return map == null ? Collections.<K>emptySet() : map.keySet();
   }

   public Collection<V> values() {
      AtomicHashMap<K, V> map = getDeltaMapForRead();
      return map == null ? Collections.<V>emptySet() : map.values();
   }

   public Set<Entry<K, V>> entrySet() {
      AtomicHashMap<K, V> map = getDeltaMapForRead();
      return map == null ? Collections.<Entry<K,V>>emptySet() : map.entrySet();
   }

   public int size() {
      AtomicHashMap<K, V> map = getDeltaMapForRead();
      return map == null ? 0 : map.size();
   }

   public boolean isEmpty() {
      AtomicHashMap<K, V> map = getDeltaMapForRead();
      return map == null || map.isEmpty();
   }

   public boolean containsKey(Object key) {
      AtomicHashMap<K, V> map = getDeltaMapForRead();
      return map != null && map.containsKey(key);
   }

   public boolean containsValue(Object value) {
      AtomicHashMap<K, V> map = getDeltaMapForRead();
      return map != null && map.containsValue(value);
   }

   public V get(Object key) {
      AtomicHashMap<K, V> map = getDeltaMapForRead();
      return map == null ? null : map.get(key);
   }

   // writers

   public V put(K key, V value) {
      try {
         startAtomic();
         InvocationContext ctx = icc.createInvocationContext(true);
         AtomicHashMap<K, V> deltaMapForWrite = getDeltaMapForWrite(ctx);
         return deltaMapForWrite.put(key, value);
      }
      finally {
         endAtomic();
      }
   }

   public V remove(Object key) {
      try {
         startAtomic();
         InvocationContext ic = icc.createInvocationContext(true);
         return getDeltaMapForWrite(ic).remove(key);
      }
      finally {
         endAtomic();
      }
   }

   public void putAll(Map<? extends K, ? extends V> m) {
      try {
         startAtomic();
         InvocationContext ic = icc.createInvocationContext(true);
         getDeltaMapForWrite(ic).putAll(m);
      }
      finally {
         endAtomic();
      }
   }

   public void clear() {
      try {
         startAtomic();
         InvocationContext ic = icc.createInvocationContext(true);
         getDeltaMapForWrite(ic).clear();
      }
      finally {
         endAtomic();
      }
   }

   @Override
   public String toString() {
      return "AtomicHashMapProxy{" +
            "deltaMapKey=" + deltaMapKey +
            '}';
   }
}
