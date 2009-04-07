/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
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
import org.infinispan.invocation.InvocationContextContainer;
import org.infinispan.invocation.Flag;
import org.infinispan.logging.Log;
import org.infinispan.logging.LogFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * A layer of indirection around an {@link org.infinispan.atomic.AtomicHashMap} to provide reader consistency
 *
 * @author Manik Surtani (<a href="mailto:manik AT jboss DOT org">manik AT jboss DOT org</a>)
 * @since 4.0
 */
public class AtomicHashMapProxy<K, V> extends AutoBatchSupport implements AtomicMap<K, V> {
   private static final Log log = LogFactory.getLog(AtomicHashMapProxy.class);
   private static final boolean trace = log.isTraceEnabled();
   Object deltaMapKey;
   Cache cache;
   InvocationContextContainer icc;

   public AtomicHashMapProxy(Cache cache, Object deltaMapKey, BatchContainer batchContainer, InvocationContextContainer icc) {
      this.cache = cache;
      this.deltaMapKey = deltaMapKey;
      this.batchContainer = batchContainer;
      this.icc = icc;
   }

   // internal helper, reduces lots of casts.
   private AtomicHashMap<K, V> getDeltaMapForRead() {
      return (AtomicHashMap<K, V>) cache.get(deltaMapKey);
   }

   private AtomicHashMap<K, V> getDeltaMapForWrite() {
      if (ownsLock()) {
         return (AtomicHashMap<K, V>) cache.get(deltaMapKey);
      } else {
         // acquire WL
         boolean suppressLocks = icc.get().hasFlag(Flag.SKIP_LOCKING);
         if (!suppressLocks) icc.get().setFlags(Flag.FORCE_WRITE_LOCK);

         if (trace) {
            if (suppressLocks)
               log.trace("Skip locking flag used.  Skipping locking.");
            else
               log.trace("Forcing write lock even for reads");
         }

         AtomicHashMap map = getDeltaMapForRead();
         // copy for write
         AtomicHashMap copy = map == null ? new AtomicHashMap() : map.copyForWrite();
         copy.initForWriting();
         // reinstate the flag
         if (suppressLocks) icc.get().setFlags(Flag.SKIP_LOCKING);
         cache.put(deltaMapKey, copy);
         return copy;
      }
   }

   private boolean ownsLock() {
      return icc.get().hasLockedKey(deltaMapKey);
   }

   // readers

   public Set<K> keySet() {
      return getDeltaMapForRead().keySet();
   }

   public Collection<V> values() {
      return getDeltaMapForRead().values();
   }

   public Set<Entry<K, V>> entrySet() {
      return getDeltaMapForRead().entrySet();
   }

   public int size() {
      return getDeltaMapForRead().size();
   }

   public boolean isEmpty() {
      return getDeltaMapForRead().isEmpty();
   }

   public boolean containsKey(Object key) {
      return getDeltaMapForRead().containsKey(key);
   }

   public boolean containsValue(Object value) {
      return getDeltaMapForRead().containsValue(value);
   }

   public V get(Object key) {
      return getDeltaMapForRead().get(key);
   }

   // writers

   public V put(K key, V value) {
      try {
         startAtomic();
         return getDeltaMapForWrite().put(key, value);
      }
      finally {
         endAtomic();
      }
   }

   public V remove(Object key) {
      try {
         startAtomic();
         return getDeltaMapForWrite().remove(key);
      }
      finally {
         endAtomic();
      }
   }

   public void putAll(Map<? extends K, ? extends V> m) {
      try {
         startAtomic();
         getDeltaMapForWrite().putAll(m);
      }
      finally {
         endAtomic();
      }
   }

   public void clear() {
      try {
         startAtomic();
         getDeltaMapForWrite().clear();
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
