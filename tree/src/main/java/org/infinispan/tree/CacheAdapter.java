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

import org.infinispan.AbstractDelegatingAdvancedCache;
import org.infinispan.AdvancedCache;
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
public class CacheAdapter<K, V> extends AbstractDelegatingAdvancedCache<K, V> {

   private final TreeContextContainer tcc;

   private CacheAdapter(AdvancedCache<K, V> cache, TreeContextContainer tcc) {
      super(cache);
      this.tcc = tcc;
   }

   public static <K, V> CacheAdapter<K, V> createAdapter(AdvancedCache<K, V> advancedCache, TreeContextContainer tcc) {
      if (advancedCache instanceof CacheAdapter)
         return (CacheAdapter<K, V>) advancedCache;
      else
         return new CacheAdapter<K, V>(advancedCache, tcc);
   }

   @Override
   public void putForExternalRead(K key, V value) {
      cache.withFlags(getFlags()).putForExternalRead(key, value);
   }

   @Override
   public void evict(K key) {
      cache.withFlags(getFlags()).evict(key);
   }

   @Override
   public boolean startBatch() {
      return cache.withFlags(getFlags()).startBatch();
   }

   @Override
   public void endBatch(boolean successful) {
      cache.withFlags(getFlags()).endBatch(successful);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      return cache.withFlags(getFlags()).put(key, value, lifespan, unit);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      return cache.withFlags(getFlags()).putIfAbsent(key, value, lifespan, unit);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      cache.withFlags(getFlags()).putAll(map, lifespan, unit);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      return cache.withFlags(getFlags()).replace(key, value, lifespan, unit);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      return cache.withFlags(getFlags()).replace(key, oldValue, value, lifespan, unit);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.withFlags(getFlags()).put(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.withFlags(getFlags()).putIfAbsent(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      cache.withFlags(getFlags()).putAll(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.withFlags(getFlags()).replace(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.withFlags(getFlags()).replace(key, oldValue, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value) {
      return cache.withFlags(getFlags()).putAsync(key, value);
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cache.withFlags(getFlags()).putAsync(key, value, lifespan, unit);
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.withFlags(getFlags()).putAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return cache.withFlags(getFlags()).putAllAsync(data);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return cache.withFlags(getFlags()).putAllAsync(data, lifespan, unit);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.withFlags(getFlags()).putAllAsync(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public NotifyingFuture<Void> clearAsync() {
      return cache.withFlags(getFlags()).clearAsync();
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value) {
      return cache.withFlags(getFlags()).putIfAbsentAsync(key, value);
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cache.withFlags(getFlags()).putIfAbsentAsync(key, value, lifespan, unit);
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.withFlags(getFlags()).putIfAbsentAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public NotifyingFuture<V> removeAsync(Object key) {
      return cache.withFlags(getFlags()).removeAsync(key);
   }

   @Override
   public NotifyingFuture<Boolean> removeAsync(Object key, Object value) {
      return cache.withFlags(getFlags()).removeAsync(key, value);
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value) {
      return cache.withFlags(getFlags()).replaceAsync(key, value);
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cache.withFlags(getFlags()).replaceAsync(key, value, lifespan, unit);
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.withFlags(getFlags()).replaceAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return cache.withFlags(getFlags()).replaceAsync(key, oldValue, newValue);
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return cache.withFlags(getFlags()).replaceAsync(key, oldValue, newValue, lifespan, unit);
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.withFlags(getFlags()).replaceAsync(key, oldValue, newValue, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public void compact() {
      cache.withFlags(getFlags()).compact();
   }

   @Override
   public int size() {
      return cache.withFlags(getFlags()).size();
   }

   @Override
   public boolean isEmpty() {
      return cache.withFlags(getFlags()).isEmpty();
   }

   @Override
   public boolean containsKey(Object key) {
      return cache.withFlags(getFlags()).containsKey(key);
   }

   @Override
   public boolean containsValue(Object value) {
      return cache.withFlags(getFlags()).containsValue(value);
   }

   @Override
   public V get(Object key) {
      return cache.withFlags(getFlags()).get(key);
   }

   @Override
   public V put(K key, V value) {
      return cache.withFlags(getFlags()).put(key, value);
   }

   @Override
   public V remove(Object key) {
      return cache.withFlags(getFlags()).remove(key);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m) {
      cache.withFlags(getFlags()).putAll(m);
   }

   @Override
   public void clear() {
      cache.withFlags(getFlags()).clear();
   }

   @Override
   public Set<K> keySet() {
      return cache.withFlags(getFlags()).keySet();
   }

   @Override
   public Collection<V> values() {
      return cache.withFlags(getFlags()).values();
   }

   @Override
   public Set<Entry<K, V>> entrySet() {
      return cache.withFlags(getFlags()).entrySet();
   }

   @Override
   public V putIfAbsent(K key, V value) {
      return cache.withFlags(getFlags()).putIfAbsent(key, value);
   }

   @Override
   public boolean remove(Object key, Object value) {
      return cache.withFlags(getFlags()).remove(key, value);
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      return cache.withFlags(getFlags()).replace(key, oldValue, newValue);
   }

   @Override
   public V replace(K key, V value) {
      return cache.withFlags(getFlags()).replace(key, value);
   }

   @Override
   public NotifyingFuture<V> getAsync(K key) {
      return cache.withFlags(getFlags()).getAsync(key);
   }

   private Flag[] getFlags() {
      if (tcc.getTreeContext() == null)
         return null;
      else {
         EnumSet<Flag> flagSet = tcc.getTreeContext().getFlags();
         return flagSet.toArray(new Flag[flagSet.size()]);
      }
   }

   @Override
   public AdvancedCache<K, V> withFlags(Flag... flags) {
      throw new UnsupportedOperationException("Unsupported in this implementation");
   }

   @Override
   public AdvancedCache<K, V> with(ClassLoader classLoader) {
      throw new UnsupportedOperationException("Unsupported in this implementation");
   }
}
