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

import net.jcip.annotations.NotThreadSafe;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.context.FlagContainer;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.FastCopyHashMap;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * The default implementation of {@link AtomicMap}.  Note that this map cannot be constructed directly, and callers
 * should obtain references to AtomicHashMaps via the {@link AtomicMapLookup} helper.  This helper will ensure proper
 * concurrent construction and registration of AtomicMaps in Infinispan's data container.  E.g.:
 * <br /><br />
 * <code>
 *    AtomicMap&lt;String, Integer&gt; map = AtomicMapLookup.getAtomicMap(cache, "my_atomic_map_key");
 * </code>
 * <br /><br />
 * Note that for replication to work properly, AtomicHashMap updates <b><i>must always</i></b> take place within the
 * scope of an ongoing JTA transaction or batch (see {@link Cache#startBatch()}).
 * <p/>
 *
 * @author (various)
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 * @see AtomicMap
 * @see AtomicMapLookup
 * @since 4.0
 */
@NotThreadSafe
public class AtomicHashMap<K, V> implements AtomicMap<K, V>, DeltaAware, Cloneable {
   FastCopyHashMap<K, V> delegate;
   private AtomicHashMapDelta delta = null;
   private volatile AtomicHashMapProxy<K, V> proxy;
   volatile boolean copied = false;
   volatile boolean removed = false;

   /**
    * Construction only allowed through this factory method.  This factory is intended for use internally by the
    * CacheDelegate.  User code should use {@link AtomicMapLookup#getAtomicMap(Cache, Object)}.
    */
   public static <K, V> AtomicHashMap<K, V> newInstance(Cache<Object, Object> cache, Object cacheKey) {
      AtomicHashMap<K, V> value = new AtomicHashMap<K, V>();
      Object oldValue = cache.putIfAbsent(cacheKey, value);
      if (oldValue != null) value = (AtomicHashMap<K, V>) oldValue;
      return value;
   }

   public AtomicHashMap() {
      delegate = new FastCopyHashMap<K, V>();
   }

   private AtomicHashMap(FastCopyHashMap<K, V> delegate) {
      this.delegate = delegate;
   }

   public AtomicHashMap(boolean isCopy) {
      this();
      copied = isCopy;
   }

   @Override
   public void commit() {
      copied = false;
      delta = null;
   }

   @Override
   public int size() {
      return delegate.size();
   }

   @Override
   public boolean isEmpty() {
      return delegate.isEmpty();
   }

   @Override
   public boolean containsKey(Object key) {
      return delegate.containsKey(key);
   }

   @Override
   public boolean containsValue(Object value) {
      return delegate.containsValue(value);
   }

   @Override
   public V get(Object key) {
      return delegate.get(key);
   }

   @Override
   public Set<K> keySet() {
      return delegate.keySet();
   }

   @Override
   public Collection<V> values() {
      return delegate.values();
   }

   @Override
   public Set<Entry<K, V>> entrySet() {
      return delegate.entrySet();
   }

   @Override
   public V put(K key, V value) {
      V oldValue = delegate.put(key, value);
      PutOperation<K, V> op = new PutOperation<K, V>(key, oldValue, value);
      getDelta().addOperation(op);
      return oldValue;
   }

   @Override
   @SuppressWarnings("unchecked")
   public V remove(Object key) {
      V oldValue = delegate.remove(key);
      RemoveOperation<K, V> op = new RemoveOperation<K, V>((K) key, oldValue);
      getDelta().addOperation(op);
      return oldValue;
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> t) {
      // this is crappy - need to do this more efficiently!
      for (Entry<? extends K, ? extends V> e : t.entrySet()) put(e.getKey(), e.getValue());
   }

   @Override
   @SuppressWarnings("unchecked")
   public void clear() {
      FastCopyHashMap<K, V> originalEntries = delegate.clone();
      ClearOperation<K, V> op = new ClearOperation<K, V>(originalEntries);
      getDelta().addOperation(op);
      delegate.clear();
   }

   /**
    * Builds a thread-safe proxy for this instance so that concurrent reads are isolated from writes.
    * @return an instance of AtomicHashMapProxy
    */
   AtomicHashMapProxy<K, V> getProxy(AdvancedCache<Object, Object> cache, Object mapKey, boolean fineGrained, FlagContainer flagContainer) {
      // construct the proxy lazily
      if (proxy == null)  // DCL is OK here since proxy is volatile (and we live in a post-JDK 5 world)
      {
         synchronized (this) {
            if (proxy == null)
               if(fineGrained){
                  proxy = new FineGrainedAtomicHashMapProxy<K, V>(cache, mapKey);
               } else {
                  proxy = new AtomicHashMapProxy<K, V>(cache, mapKey, flagContainer);
               }
         }
      }
      return proxy;
   }

   public void markRemoved(boolean b) {
      removed = b;
   }

   @Override
   public Delta delta() {
      Delta toReturn = delta == null ? NullDelta.INSTANCE : delta;
      delta = null; // reset
      return toReturn;
   }

   @SuppressWarnings("unchecked")
   public AtomicHashMap<K, V> copy() {
      try {
         AtomicHashMap<K, V> clone = (AtomicHashMap<K, V>) super.clone();
         clone.delegate = (FastCopyHashMap<K, V>) delegate.clone();
         clone.proxy = proxy;
         clone.copied = true;
         return clone;
      }
      catch (CloneNotSupportedException e) {
         // should never happen!!
         throw new RuntimeException(e);
      }
   }

   @Override
   public String toString() {
      StringBuilder sb = new StringBuilder("AtomicHashMap{delegate=");
      sb.append(delegate);
      sb.append("}");
      return sb.toString();    
   }

   /**
    * Initializes the delta instance to start recording changes.
    */
   public void initForWriting() {
      delta = new AtomicHashMapDelta();
   }

   AtomicHashMapDelta getDelta() {
      if (delta == null) delta = new AtomicHashMapDelta();
      return delta;
   }



   public static class Externalizer extends AbstractExternalizer<AtomicHashMap> {
      @Override
      public void writeObject(ObjectOutput output, AtomicHashMap map) throws IOException {
         output.writeObject(map.delegate);
      }

      @Override
      @SuppressWarnings("unchecked")
      public AtomicHashMap readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         FastCopyHashMap<?, ?> delegate = (FastCopyHashMap<?, ?>) input.readObject();
         return new AtomicHashMap(delegate);
      }

      @Override
      public Integer getId() {
         return Ids.ATOMIC_HASH_MAP;
      }

      @Override
      @SuppressWarnings("unchecked")
      public Set<Class<? extends AtomicHashMap>> getTypeClasses() {
         return Util.<Class<? extends AtomicHashMap>>asSet(AtomicHashMap.class);
      }
   }
}
