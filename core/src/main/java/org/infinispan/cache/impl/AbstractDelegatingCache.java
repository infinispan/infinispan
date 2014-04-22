package org.infinispan.cache.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.filter.Converter;
import org.infinispan.filter.KeyFilter;
import org.infinispan.filter.KeyValueFilter;
import org.infinispan.commons.util.concurrent.NotifyingFuture;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * This is a convenient base class for implementing a cache delegate. The only constructor takes a {@link Cache}
 * argument, to which each method call is delegated. One can extend this class and override the method sub-set it is
 * interested in. There is also an similar implementation for {@link org.infinispan.AdvancedCache}: {@link
 * org.infinispan.cache.impl.AbstractDelegatingAdvancedCache}.
 *
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.cache.impl.AbstractDelegatingAdvancedCache
 */
public abstract class AbstractDelegatingCache<K, V> implements Cache<K, V> {

   private final Cache<K, V> cache;

   public AbstractDelegatingCache(Cache<K, V> cache) {
      this.cache = cache;
      if (cache == null) throw new IllegalArgumentException("Delegate cache cannot be null!");
   }

   @Override
   public void putForExternalRead(K key, V value) {
      cache.putForExternalRead(key, value);
   }

   @Override
   public void evict(K key) {
      cache.evict(key);
   }

   @Override
   public org.infinispan.configuration.cache.Configuration getCacheConfiguration() {
      return cache.getCacheConfiguration();
   }

   @Override
   public boolean startBatch() {
      return cache.startBatch();
   }

   @Override
   public void endBatch(boolean successful) {
      cache.endBatch(successful);
   }

   @Override
   public String getName() {
      return cache.getName();
   }

   @Override
   public String getVersion() {
      return cache.getVersion();
   }

   @Override
   public EmbeddedCacheManager getCacheManager() {
      return cache.getCacheManager();
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      return cache.put(key, value, lifespan, unit);
   }

   /**
    * Don't remove.
    * @see {@link org.infinispan.cache.impl.CacheSupport#set(Object, Object)}
    */
   protected void set(K key, V value) {
      cache.put(key, value);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      return cache.putIfAbsent(key, value, lifespan, unit);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      cache.putAll(map, lifespan, unit);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      return cache.replace(key, value, lifespan, unit);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      return cache.replace(key, oldValue, value, lifespan, unit);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.put(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.putIfAbsent(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      cache.putAll(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.replace(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return cache.replace(key, oldValue, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value) {
      return cache.putAsync(key, value);
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cache.putAsync(key, value, lifespan, unit);
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.putAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return cache.putAllAsync(data);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return cache.putAllAsync(data, lifespan, unit);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.putAllAsync(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public NotifyingFuture<Void> clearAsync() {
      return cache.clearAsync();
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value) {
      return cache.putIfAbsentAsync(key, value);
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cache.putIfAbsentAsync(key, value, lifespan, unit);
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.putIfAbsentAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public NotifyingFuture<V> removeAsync(Object key) {
      return cache.removeAsync(key);
   }

   @Override
   public NotifyingFuture<Boolean> removeAsync(Object key, Object value) {
      return cache.removeAsync(key, value);
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value) {
      return cache.replaceAsync(key, value);
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return cache.replaceAsync(key, value, lifespan, unit);
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.replaceAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return cache.replaceAsync(key, oldValue, newValue);
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return cache.replaceAsync(key, oldValue, newValue, lifespan, unit);
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return cache.replaceAsync(key, oldValue, newValue, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public AdvancedCache<K, V> getAdvancedCache() {
      return cache.getAdvancedCache();
   }

   @Override
   public ComponentStatus getStatus() {
      return cache.getStatus();
   }

   @Override
   public V putIfAbsent(K key, V value) {
      return cache.putIfAbsent(key, value);
   }

   @Override
   public boolean remove(Object key, Object value) {
      return cache.remove(key, value);
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      return cache.replace(key, oldValue, newValue);
   }

   @Override
   public V replace(K key, V value) {
      return cache.replace(key, value);
   }

   @Override
   public int size() {
      return cache.size();
   }

   @Override
   public boolean isEmpty() {
      return cache.isEmpty();
   }

   @Override
   public boolean containsKey(Object key) {
      return cache.containsKey(key);
   }

   @Override
   public boolean containsValue(Object value) {
      return cache.containsValue(value);
   }

   @Override
   public V get(Object key) {
      return cache.get(key);
   }

   @Override
   public V put(K key, V value) {
      return cache.put(key, value);
   }

   @Override
   public V remove(Object key) {
      return cache.remove(key);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> t) {
      cache.putAll(t);
   }

   @Override
   public void clear() {
      cache.clear();
   }

   @Override
   public Set<K> keySet() {
      return cache.keySet();
   }

   @Override
   public Set<Entry<K, V>> entrySet() {
      return cache.entrySet();
   }

   @Override
   public Collection<V> values() {
      return cache.values();
   }

   @Override
   public void start() {
      cache.start();
   }

   @Override
   public void stop() {
      cache.stop();
   }

   @Override
   public void addListener(Object listener) {
      cache.addListener(listener);
   }

   @Override
   public void addListener(Object listener, KeyFilter<? super K> filter) {
      cache.addListener(listener, filter);
   }

   @Override
   public <C> void addListener(Object listener, KeyValueFilter<? super K, ? super V> filter,
                               Converter<? super K, ? super V, C> converter) {
      cache.addListener(listener, filter, converter);
   }

   @Override
   public void removeListener(Object listener) {
      cache.removeListener(listener);
   }

   @Override
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

   public Cache<K, V> getDelegate() {
      return cache;
   }
}
