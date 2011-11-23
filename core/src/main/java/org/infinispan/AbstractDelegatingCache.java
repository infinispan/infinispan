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
package org.infinispan;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.api.util.concurrent.NotifyingFuture;
import org.infinispan.config.Configuration;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * This is a convenient base class for implementing a cache delegate. The only constructor takes a {@link Cache}
 * argument, to which each method call is delegated. One can extend this class and override the method sub-set it is
 * interested in. There is also an similar implementation for {@link org.infinispan.AdvancedCache}: {@link
 * org.infinispan.AbstractDelegatingAdvancedCache}.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.AbstractDelegatingAdvancedCache
 */
public abstract class AbstractDelegatingCache<K, V> implements Cache<K, V> {

   private Cache<K, V> cache;

   public AbstractDelegatingCache(Cache<K, V> cache) {
      this.cache = cache;
      if (cache == null) throw new IllegalArgumentException("Delegate cache cannot be null!");
   }

   public void putForExternalRead(K key, V value) {
      cache.putForExternalRead(key, value);
   }

   public void evict(K key) {
      cache.evict(key);
   }

   public Configuration getConfiguration() {
      return cache.getConfiguration();
   }

   public boolean startBatch() {
      return cache.startBatch();
   }

   public void endBatch(boolean successful) {
      cache.endBatch(successful);
   }

   public String getName() {
      return cache.getName();
   }

   public String getVersion() {
      return cache.getVersion();
   }

   public EmbeddedCacheManager getCacheManager() {
      return cache.getCacheManager();
   }

   public V put(K key, V value, long lifespan, TimeUnit unit) {
      return cache.put(key, value, lifespan, unit);
   }

   /**
    * Don't remove.
    * @see {@link org.infinispan.CacheSupport#set(Object, Object)}
    */
   protected void set(K key, V value) {
      cache.put(key, value);
   }

   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      return cache.putIfAbsent(key, value, lifespan, unit);
   }

   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      cache.putAll(map, lifespan, unit);
   }

   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      return cache.replace(key, value, lifespan, unit);
   }

   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      return cache.replace(key, oldValue, value, lifespan, unit);
   }

   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.put(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.putIfAbsent(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      cache.putAll(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.replace(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.replace(key, oldValue, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   public NotifyingFuture<V> putAsync(K key, V value) {
      return cache.putAsync(key, value);
   }

   public NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cache.putAsync(key, value, lifespan, unit);
   }

   public NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.putAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return cache.putAllAsync(data);
   }

   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return cache.putAllAsync(data, lifespan, unit);
   }

   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.putAllAsync(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   public NotifyingFuture<Void> clearAsync() {
      return cache.clearAsync();
   }

   public NotifyingFuture<V> putIfAbsentAsync(K key, V value) {
      return cache.putIfAbsentAsync(key, value);
   }

   public NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cache.putIfAbsentAsync(key, value, lifespan, unit);
   }

   public NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.putIfAbsentAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   public NotifyingFuture<V> removeAsync(Object key) {
      return cache.removeAsync(key);
   }

   public NotifyingFuture<Boolean> removeAsync(Object key, Object value) {
      return cache.removeAsync(key, value);
   }

   public NotifyingFuture<V> replaceAsync(K key, V value) {
      return cache.replaceAsync(key, value);
   }

   public NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cache.replaceAsync(key, value, lifespan, unit);
   }

   public NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.replaceAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return cache.replaceAsync(key, oldValue, newValue);
   }

   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return cache.replaceAsync(key, oldValue, newValue, lifespan, unit);
   }

   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.replaceAsync(key, oldValue, newValue, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   public AdvancedCache<K, V> getAdvancedCache() {
      return cache.getAdvancedCache();
   }

   public void compact() {
      cache.compact();
   }

   public ComponentStatus getStatus() {
      return cache.getStatus();
   }

   public V putIfAbsent(K key, V value) {
      return cache.putIfAbsent(key, value);
   }

   public boolean remove(Object key, Object value) {
      return cache.remove(key, value);
   }

   public boolean replace(K key, V oldValue, V newValue) {
      return cache.replace(key, oldValue, newValue);
   }

   public V replace(K key, V value) {
      return cache.replace(key, value);
   }

   public int size() {
      return cache.size();
   }

   public boolean isEmpty() {
      return cache.isEmpty();
   }

   public boolean containsKey(Object key) {
      return cache.containsKey(key);
   }

   public boolean containsValue(Object value) {
      return cache.containsValue(value);
   }

   public V get(Object key) {
      return cache.get(key);
   }

   public V put(K key, V value) {
      return cache.put(key, value);
   }

   public V remove(Object key) {
      return cache.remove(key);
   }

   public void putAll(Map<? extends K, ? extends V> t) {
      cache.putAll(t);
   }

   public void clear() {
      cache.clear();
   }

   public Set<K> keySet() {
      return cache.keySet();
   }

   public Set<Entry<K, V>> entrySet() {
      return cache.entrySet();
   }

   public Collection<V> values() {
      return cache.values();
   }

   public void start() {
      cache.start();
   }

   public void stop() {
      cache.stop();
   }

   public void addListener(Object listener) {
      cache.addListener(listener);
   }

   public void removeListener(Object listener) {
      cache.removeListener(listener);
   }

   public Set<Object> getListeners() {
      return cache.getListeners();
   }

   @Override
   public NotifyingFuture<V> getAsync(K key) {
      return cache.getAsync(key);
   }
   
   @Override
   public String toString() {
      return cache.toString();
   }
}
