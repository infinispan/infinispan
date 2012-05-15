/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

package org.infinispan.tree;

import org.infinispan.AbstractDelegatingCacheImpl;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.DecoratedCache;
import org.infinispan.context.Flag;
import org.infinispan.util.concurrent.NotifyingFuture;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A cache adapter for setting flags for each individual cache operation called
 * within the scope of a TreeCache operation.
 *
 * Note: We can't just wrap around use of cache in tree module because flag
 * assignment needs to extend even to calls within AtomicHashMapProxy.getDeltaMapForWrite().
 * So, the only possible way is to wrap around the Cache instance around
 * something that sets flags accordingly.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
public class CacheAdapter<K, V> extends AbstractDelegatingCacheImpl<K, V> {

   final TreeContextContainer tcc;

   CacheAdapter(AdvancedCache<K, V> cache, TreeContextContainer tcc) {
      super(cache);
      this.tcc = tcc;
   }

   public static <K, V> CacheAdapter<K, V> createAdapter(AdvancedCache<K, V> advancedCache, TreeContextContainer tcc) {
      if (advancedCache instanceof CacheAdapter) {
         return (CacheAdapter<K, V>) advancedCache;
      } else if (advancedCache instanceof DecoratedCache) {
         DecoratedCache dc = (DecoratedCache) advancedCache;
         Cache<K, V> delegate = dc.getDelegate();
         if (delegate instanceof AdvancedCache)
            return new DecoratedCacheAdapter<K, V>((AdvancedCache) delegate, tcc, dc.getFlags());
         else
            return new DecoratedCacheAdapter<K, V>(delegate.getAdvancedCache(), tcc, dc.getFlags());
      } else {
         return new CacheAdapter<K, V>(advancedCache, tcc);
      }
   }

   @Override
   public void putForExternalRead(K key, V value) {
      super.putForExternalRead(key, value, getFlags(), null);
   }

   @Override
   public void evict(K key) {
      super.evict(key, getFlags(), null);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      return super.put(key, value, lifespan, unit, getFlags(), null);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      return super.putIfAbsent(key, value, lifespan, unit, getFlags(), null);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      super.putAll(map, lifespan, unit, getFlags(), null);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      return super.replace(key, value, lifespan, unit, getFlags(), null);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      return super.replace(key, oldValue, value, lifespan, unit, getFlags(), null);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return super.put(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, getFlags(), null);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return super.putIfAbsent(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, getFlags(), null);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      super.putAll(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, getFlags(), null);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return super.replace(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, getFlags(), null);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return super.replace(key, oldValue, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, getFlags(), null);
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value) {
      return super.putAsync(key, value, getFlags(), null);
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return super.putAsync(key, value, lifespan, unit, getFlags(), null);
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return super.putAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit, getFlags(), null);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return super.putAllAsync(data, getFlags(), null);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return super.putAllAsync(data, lifespan, unit, getFlags(), null);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return super.putAllAsync(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit, getFlags(), null);
   }

   @Override
   public NotifyingFuture<Void> clearAsync() {
      return super.clearAsync(getFlags(), null);
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value) {
      return super.putIfAbsentAsync(key, value, getFlags(), null);
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      return super.putIfAbsentAsync(key, value, lifespan, unit, getFlags(), null);
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return super.putIfAbsentAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit, getFlags(), null);
   }

   @Override
   public NotifyingFuture<V> removeAsync(Object key) {
      return super.removeAsync(key, getFlags(), null);
   }

   @Override
   public NotifyingFuture<Boolean> removeAsync(Object key, Object value) {
      return super.removeAsync(key, value, getFlags(), null);
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value) {
      return super.replaceAsync(key, value, getFlags(), null);
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return super.replaceAsync(key, value, lifespan, unit, getFlags(), null);
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return super.replaceAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit, getFlags(), null);
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return super.replaceAsync(key, oldValue, newValue, getFlags(), null);
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return super.replaceAsync(key, oldValue, newValue, lifespan, unit, getFlags(), null);
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return super.replaceAsync(key, oldValue, newValue, lifespan, lifespanUnit, maxIdle, maxIdleUnit, getFlags(), null);
   }

   @Override
   public int size() {
      return super.size(getFlags(), null);
   }

   @Override
   public boolean isEmpty() {
      return super.isEmpty(getFlags(), null);
   }

   @Override
   public boolean containsKey(Object key) {
      return super.containsKey(key, getFlags(), null);
   }

   @Override
   public V get(Object key) {
      return super.get(key, getFlags(), null);
   }

   @Override
   public V put(K key, V value) {
      return super.put(key, value, getFlags(), null);
   }

   @Override
   public V remove(Object key) {
      return super.remove(key, getFlags(), null);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m) {
      super.putAll(m, getFlags(), null);
   }

   @Override
   public void clear() {
      super.clear(getFlags(), null);
   }

   @Override
   public Set<K> keySet() {
      return super.keySet(getFlags(), null);
   }

   @Override
   public Collection<V> values() {
      return super.values(getFlags(), null);
   }

   @Override
   public Set<Entry<K, V>> entrySet() {
      return super.entrySet(getFlags(), null);
   }

   @Override
   public V putIfAbsent(K key, V value) {
      return super.putIfAbsent(key, value, getFlags(), null);
   }

   @Override
   public boolean remove(Object key, Object value) {
      return super.remove(key, value, getFlags(), null);
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      return super.replace(key, oldValue, newValue, getFlags(), null);
   }

   @Override
   public V replace(K key, V value) {
      return super.replace(key, value, getFlags(), null);
   }

   @Override
   public NotifyingFuture<V> getAsync(K key) {
      return super.getAsync(key, getFlags(), null);
   }

   protected EnumSet<Flag> getFlags() {
      if (tcc.getTreeContext() == null)
         return null;
      else {
         return tcc.getTreeContext().getFlags();
      }
   }

   @Override
   public AdvancedCache<K, V> withFlags(Flag... flags) {
      return new DecoratedCacheAdapter<K, V>(cache, tcc, flags);
   }

   @Override
   public AdvancedCache<K, V> with(ClassLoader classLoader) {
      throw new UnsupportedOperationException("Unsupported in this implementation");
   }
}
