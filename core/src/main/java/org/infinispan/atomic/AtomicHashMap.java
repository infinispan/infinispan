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

import net.jcip.annotations.NotThreadSafe;
import org.infinispan.Cache;
import org.infinispan.batch.BatchContainer;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.util.FastCopyHashMap;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Note that for replication to work properly, this class <b><i>requires</i></b> that all writes take place within the
 * scope of an ongoing transaction or batch.
 * <p/>
 *
 * @author (various)
 * @param <K>
 * @param <V>
 * @since 4.0
 */
@NotThreadSafe
public class AtomicHashMap<K, V> implements AtomicMap<K, V>, DeltaAware, Cloneable {
   FastCopyHashMap<K, V> delegate;
   private AtomicHashMapDelta delta = null;
   private volatile AtomicHashMapProxy proxy;

   /**
    * Construction only allowed through this factory method.  This factory is intended for use internally by the
    * CacheDelegate.  User code should use {@link org.infinispan.atomic.AtomicMapCache#getAtomicMap(Object)}.
    */
   public static AtomicHashMap newInstance(Cache cache, Object cacheKey) {
      AtomicHashMap value = new AtomicHashMap();
      Object oldValue = cache.putIfAbsent(cacheKey, value);
      if (oldValue != null) value = (AtomicHashMap) oldValue;
      return value;
   }

   public AtomicHashMap() {
      delegate = new FastCopyHashMap<K, V>();
   }

   public void commit() {
      if (delta != null) delta = null;
   }

   public int size() {
      return delegate.size();
   }

   public boolean isEmpty() {
      return delegate.isEmpty();
   }

   public boolean containsKey(Object key) {
      return delegate.containsKey(key);
   }

   public boolean containsValue(Object value) {
      return delegate.containsValue(value);
   }

   public V get(Object key) {
      return delegate.get(key);
   }

   public Set<K> keySet() {
      return delegate.keySet();
   }

   public Collection<V> values() {
      return delegate.values();
   }

   public Set<Entry<K, V>> entrySet() {
      return delegate.entrySet();
   }

   public V put(K key, V value) {
      V oldValue = delegate.put(key, value);
      PutOperation<K, V> op = new PutOperation<K, V>(key, oldValue, value);
      getDelta().addOperation(op);
      return oldValue;
   }

   public V remove(Object key) {
      V oldValue = delegate.remove(key);
      RemoveOperation<K, V> op = new RemoveOperation<K, V>((K)key, oldValue);
      getDelta().addOperation(op);
      return oldValue;
   }

   public void putAll(Map<? extends K, ? extends V> t) {
      // this is crappy - need to do this more efficiently!
      for (Entry<? extends K, ? extends V> e : t.entrySet()) put(e.getKey(), e.getValue());
   }

   public void clear() {
      FastCopyHashMap<K, V> originalEntries = (FastCopyHashMap<K, V>) delegate.clone();
      ClearOperation<K, V> op = new ClearOperation<K, V>(originalEntries);
      if (delta!= null ) delta.addOperation(op);
      delegate.clear();
   }

   public AtomicMap getProxy(Cache cache, Object mapKey,
                             BatchContainer batchContainer, InvocationContextContainer icc) {
      // construct the proxy lazily
      if (proxy == null)  // DCL is OK here since proxy is volatile (and we live in a post-JDK 5 world)
      {
         synchronized (this) {
            if (proxy == null)
               proxy = new AtomicHashMapProxy(cache, mapKey, batchContainer, icc);
         }
      }
      return proxy;
   }

   public Delta delta() {
      Delta toReturn = delta == null ? NullDelta.INSTANCE : delta;
      delta = null; // reset
      return toReturn;
   }

   public AtomicHashMap copyForWrite() {
      try {
         AtomicHashMap clone = (AtomicHashMap) super.clone();
         clone.delegate = (FastCopyHashMap) delegate.clone();
         clone.proxy = proxy;
         return clone;
      }
      catch (CloneNotSupportedException e) {
         // should never happen!!
         throw new RuntimeException(e);
      }
   }

   @Override
   public String toString() {
      return "AtomicHashMap{" +
            "delegate=" + delegate +
            '}';
   }

   /**
    * Initializes the delta instance to start recording changes.
    */
   public void initForWriting() {
      delta = new AtomicHashMapDelta();
   }

   private AtomicHashMapDelta getDelta() {
      if (delta == null) delta = new AtomicHashMapDelta();
      return delta;
   }
}
