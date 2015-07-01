package org.infinispan.jcache.remote;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.concurrent.NotifyingFuture;

/**
 * Base class for building wrappers over remote cache instances.
 */
abstract class RemoteCacheWrapper<K, V> implements RemoteCache<K, V> {
   protected final RemoteCache<K, V> delegate;

   public RemoteCacheWrapper(RemoteCache<K, V> delegate) {
      this.delegate = delegate;
   }

   @Override
   public void addClientListener(Object listener) {
      delegate.addClientListener(listener);
   }

   @Override
   public void addClientListener(Object listener, Object[] filterFactoryParams, Object[] converterFactoryParams) {
      delegate.addClientListener(listener, filterFactoryParams, converterFactoryParams);
   }

   @Override
   public void clear() {
      delegate.clear();
   }

   @Override
   public NotifyingFuture<Void> clearAsync() {
      return delegate.clearAsync();
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
   public Set<java.util.Map.Entry<K, V>> entrySet() {
      return delegate.entrySet();
   }

   @Override
   public V get(Object key) {
      return delegate.get(key);
   }

   @Override
   public Map<K, V> getAll(Set<? extends K> keys) {
      return delegate.getAll(keys);
   }

   @Override
   public NotifyingFuture<V> getAsync(K key) {
      return delegate.getAsync(key);
   }

   @Override
   public Map<K, V> getBulk() {
      return delegate.getBulk();
   }

   @Override
   public Map<K, V> getBulk(int size) {
      return delegate.getBulk(size);
   }

   @Override
   public Set<Object> getListeners() {
      return delegate.getListeners();
   }

   @Override
   public String getName() {
      return delegate.getName();
   }

   @Override
   public String getProtocolVersion() {
      return delegate.getProtocolVersion();
   }

   @Override
   public RemoteCacheManager getRemoteCacheManager() {
      return delegate.getRemoteCacheManager();
   }

   @Override
   public String getVersion() {
      return delegate.getVersion();
   }

   @Override
   public VersionedValue<V> getVersioned(K key) {
      return delegate.getVersioned(key);
   }

   @Override
   public MetadataValue<V> getWithMetadata(K key) {
      return delegate.getWithMetadata(key);
   }

   @Override
   public boolean isEmpty() {
      return delegate.isEmpty();
   }

   @Override
   public Set<K> keySet() {
      return delegate.keySet();
   }

   @Override
   public V put(K key, V value) {
      return delegate.put(key, value);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit unit) {
      return delegate.put(key, value, lifespan, unit);
   }

   @Override
   public V put(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return delegate.put(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> m) {
      delegate.putAll(m);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit unit) {
      delegate.putAll(map, lifespan, unit);
   }

   @Override
   public void putAll(Map<? extends K, ? extends V> map, long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
         TimeUnit maxIdleTimeUnit) {
      delegate.putAll(map, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return delegate.putAllAsync(data);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return delegate.putAllAsync(data, lifespan, unit);
   }

   @Override
   public NotifyingFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit,
         long maxIdle, TimeUnit maxIdleUnit) {
      return delegate.putAllAsync(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value) {
      return delegate.putAsync(key, value);
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return delegate.putAsync(key, value, lifespan, unit);
   }

   @Override
   public NotifyingFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle,
         TimeUnit maxIdleUnit) {
      return delegate.putAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public V putIfAbsent(K key, V value) {
      return delegate.putIfAbsent(key, value);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit unit) {
      return delegate.putIfAbsent(key, value, lifespan, unit);
   }

   @Override
   public V putIfAbsent(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return delegate.putIfAbsent(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value) {
      return delegate.putIfAbsentAsync(key, value);
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      return delegate.putIfAbsentAsync(key, value, lifespan, unit);
   }

   @Override
   public NotifyingFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle,
         TimeUnit maxIdleUnit) {
      return delegate.putIfAbsentAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public V remove(Object key) {
      return delegate.remove(key);
   }

   @Override
   public boolean remove(Object key, Object oldValue) {
      return delegate.remove(key, oldValue);
   }

   @Override
   public NotifyingFuture<V> removeAsync(Object key) {
      return delegate.removeAsync(key);
   }

   @Override
   public NotifyingFuture<Boolean> removeAsync(Object key, Object value) {
      return delegate.removeAsync(key, value);
   }

   @Override
   public void removeClientListener(Object listener) {
      delegate.removeClientListener(listener);
   }

   @Override
   public boolean removeWithVersion(K key, long version) {
      return delegate.removeWithVersion(key, version);
   }

   @Override
   public NotifyingFuture<Boolean> removeWithVersionAsync(K key, long version) {
      return delegate.removeWithVersionAsync(key, version);
   }

   @Override
   public V replace(K key, V value) {
      return delegate.replace(key, value);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit unit) {
      return delegate.replace(key, value, lifespan, unit);
   }

   @Override
   public V replace(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return delegate.replace(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public boolean replace(K key, V oldValue, V newValue) {
      return delegate.replace(key, oldValue, newValue);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit unit) {
      return delegate.replace(key, oldValue, value, lifespan, unit);
   }

   @Override
   public boolean replace(K key, V oldValue, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime,
         TimeUnit maxIdleTimeUnit) {
      return delegate.replace(key, oldValue, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value) {
      return delegate.replaceAsync(key, value);
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return delegate.replaceAsync(key, value, lifespan, unit);
   }

   @Override
   public NotifyingFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle,
         TimeUnit maxIdleUnit) {
      return delegate.replaceAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return delegate.replaceAsync(key, oldValue, newValue);
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return delegate.replaceAsync(key, oldValue, newValue, lifespan, unit);
   }

   @Override
   public NotifyingFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit,
         long maxIdle, TimeUnit maxIdleUnit) {
      return delegate.replaceAsync(key, oldValue, newValue, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public boolean replaceWithVersion(K key, V newValue, long version) {
      return delegate.replaceWithVersion(key, newValue, version);
   }

   @Override
   public boolean replaceWithVersion(K key, V newValue, long version, int lifespanSeconds) {
      return delegate.replaceWithVersion(key, newValue, version, lifespanSeconds);
   }

   @Override
   public boolean replaceWithVersion(K key, V newValue, long version, int lifespanSeconds, int maxIdleTimeSeconds) {
      return delegate.replaceWithVersion(key, newValue, version, lifespanSeconds, maxIdleTimeSeconds);
   }

   @Override
   public boolean replaceWithVersion(K key, V newValue, long version, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      return delegate.replaceWithVersion(key, newValue, version, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
   }

   @Override
   public NotifyingFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version) {
      return delegate.replaceWithVersionAsync(key, newValue, version);
   }

   @Override
   public NotifyingFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, int lifespanSeconds) {
      return delegate.replaceWithVersionAsync(key, newValue, version, lifespanSeconds);
   }

   @Override
   public NotifyingFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, int lifespanSeconds,
         int maxIdleSeconds) {
      return delegate.replaceWithVersionAsync(key, newValue, version, lifespanSeconds, maxIdleSeconds);
   }

   @Override
   public int size() {
      return delegate.size();
   }

   @Override
   public void start() {
      delegate.start();
   }

   @Override
   public ServerStatistics stats() {
      return delegate.stats();
   }

   @Override
   public void stop() {
      delegate.stop();
   }

   @Override
   public Collection<V> values() {
      return delegate.values();
   }

   @Override
   public RemoteCache<K, V> withFlags(Flag... flags) {
      delegate.withFlags(flags);
      return this;
   }

   @Override
   public <T> T execute(String scriptName, Map<String, ?> params) {
      return delegate.execute(scriptName, params);
   }

   @Override
   public CacheTopologyInfo getCacheTopologyInfo() {
      return delegate.getCacheTopologyInfo();
   }

   @Override
   public CloseableIterator<Entry<Object, Object>> retrieveEntries(String filterConverterFactory, Set<Integer> segments, int batchSize) {
      return delegate.retrieveEntries(filterConverterFactory, segments, batchSize);
   }

   @Override
   public CloseableIterator<Entry<Object, Object>> retrieveEntries(String filterConverterFactory, int batchSize) {
      return delegate.retrieveEntries(filterConverterFactory, batchSize);
   }
}
