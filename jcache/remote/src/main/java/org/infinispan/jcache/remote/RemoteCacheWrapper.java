package org.infinispan.jcache.remote;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.StreamingRemoteCache;
import org.infinispan.client.hotrod.jmx.RemoteCacheClientStatisticsMXBean;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorCollection;
import org.infinispan.commons.util.CloseableIteratorSet;
import org.infinispan.commons.util.IntSet;
import org.infinispan.query.dsl.Query;
import org.reactivestreams.Publisher;

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
   public CompletableFuture<Void> clearAsync() {
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
   public CloseableIteratorSet<Entry<K, V>> entrySet() {
      return delegate.entrySet();
   }

   @Override
   public CloseableIteratorSet<Entry<K, V>> entrySet(IntSet segments) {
      return delegate.entrySet(segments);
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
   public CompletableFuture<V> getAsync(K key) {
      return delegate.getAsync(key);
   }

   @Override
   public CompletableFuture<MetadataValue<V>> getWithMetadataAsync(K key) {
      return delegate.getWithMetadataAsync(key);
   }

   @Deprecated
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
   public MetadataValue<V> getWithMetadata(K key) {
      return delegate.getWithMetadata(key);
   }

   @Override
   public boolean isEmpty() {
      return delegate.isEmpty();
   }

   @Override
   public CloseableIteratorSet<K> keySet() {
      return delegate.keySet();
   }

   @Override
   public CloseableIteratorSet<K> keySet(IntSet segments) {
      return delegate.keySet(segments);
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
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data) {
      return delegate.putAllAsync(data);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit unit) {
      return delegate.putAllAsync(data, lifespan, unit);
   }

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit,
                                              long maxIdle, TimeUnit maxIdleUnit) {
      return delegate.putAllAsync(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value) {
      return delegate.putAsync(key, value);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit unit) {
      return delegate.putAsync(key, value, lifespan, unit);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle,
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
   public CompletableFuture<V> putIfAbsentAsync(K key, V value) {
      return delegate.putIfAbsentAsync(key, value);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit unit) {
      return delegate.putIfAbsentAsync(key, value, lifespan, unit);
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle,
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
   public CompletableFuture<V> removeAsync(Object key) {
      return delegate.removeAsync(key);
   }

   @Override
   public CompletableFuture<Boolean> removeAsync(Object key, Object value) {
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
   public CompletableFuture<Boolean> removeWithVersionAsync(K key, long version) {
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
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return delegate.compute(key, remappingFunction);
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return delegate.compute(key, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public V compute(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return delegate.compute(key, remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return delegate.computeIfPresent(key, remappingFunction);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return delegate.computeIfPresent(key, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public V computeIfPresent(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return delegate.computeIfPresent(key, remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
      return delegate.computeIfAbsent(key, mappingFunction);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return delegate.computeIfAbsent(key, mappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return delegate.computeIfAbsent(key, mappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return delegate.merge(key, value, remappingFunction);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan,
                  TimeUnit lifespanUnit) {
      return delegate.merge(key, value, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public V merge(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan,
                  TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return delegate.merge(key, value, remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value) {
      return delegate.replaceAsync(key, value);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit unit) {
      return delegate.replaceAsync(key, value, lifespan, unit);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle,
                                            TimeUnit maxIdleUnit) {
      return delegate.replaceAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue) {
      return delegate.replaceAsync(key, oldValue, newValue);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit unit) {
      return delegate.replaceAsync(key, oldValue, newValue, lifespan, unit);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit,
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
   public CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version) {
      return delegate.replaceWithVersionAsync(key, newValue, version);
   }

   @Override
   public CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, int lifespanSeconds) {
      return delegate.replaceWithVersionAsync(key, newValue, version, lifespanSeconds);
   }

   @Override
   public CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, int lifespanSeconds,
                                                             int maxIdleSeconds) {
      return delegate.replaceWithVersionAsync(key, newValue, version, lifespanSeconds, maxIdleSeconds);
   }

   @Override
   public CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, long lifespanSeconds, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      return delegate.replaceWithVersionAsync(key, newValue, version, lifespanSeconds, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return delegate.computeAsync(key, remappingFunction);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return delegate.computeAsync(key, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return delegate.computeAsync(key, remappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction) {
      return delegate.computeIfAbsentAsync(key, mappingFunction);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return delegate.computeIfAbsentAsync(key, mappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return delegate.computeIfAbsentAsync(key, mappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction) {
      return delegate.computeIfPresentAsync(key, remappingFunction);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return delegate.computeIfPresentAsync(key, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return delegate.computeIfPresentAsync(key, remappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction) {
      return delegate.mergeAsync(key, value, remappingFunction);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit) {
      return delegate.mergeAsync(key, value, remappingFunction, lifespan, lifespanUnit);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return delegate.mergeAsync(key, value, remappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public int size() {
      return delegate.size();
   }

   @Override
   public CompletableFuture<Long> sizeAsync() {
      return delegate.sizeAsync();
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
   public CloseableIteratorCollection<V> values() {
      return delegate.values();
   }

   @Override
   public CloseableIteratorCollection<V> values(IntSet segments) {
      return delegate.values(segments);
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
   public <T> T execute(String scriptName, Map<String, ?> params, Object key) {
      return delegate.execute(scriptName, params, key);
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

   @Override
   public CloseableIterator<Entry<Object, Object>> retrieveEntries(String filterConverterFactory, Object[] filterConverterParams, Set<Integer> segments, int batchSize) {
      return delegate.retrieveEntries(filterConverterFactory, filterConverterParams, segments, batchSize);
   }

   @Override
   public <E> Publisher<Entry<K, E>> publishEntries(String filterConverterFactory, Object[] filterConverterParams, Set<Integer> segments, int batchSize) {
      return delegate.publishEntries(filterConverterFactory, filterConverterParams, segments, batchSize);
   }

   @Override
   public CloseableIterator<Entry<Object, Object>> retrieveEntriesByQuery(Query<?> filterQuery, Set<Integer> segments, int batchSize) {
      return delegate.retrieveEntriesByQuery(filterQuery, segments, batchSize);
   }

   @Override
   public <E> Publisher<Entry<K, E>> publishEntriesByQuery(Query<?> filterQuery, Set<Integer> segments, int batchSize) {
      return delegate.publishEntriesByQuery(filterQuery, segments, batchSize);
   }

   @Override
   public CloseableIterator<Entry<Object, MetadataValue<Object>>> retrieveEntriesWithMetadata(Set<Integer> segments, int batchSize) {
      return delegate.retrieveEntriesWithMetadata(segments, batchSize);
   }

   @Override
   public Publisher<Entry<K, MetadataValue<V>>> publishEntriesWithMetadata(Set<Integer> segments, int batchSize) {
      return delegate.publishEntriesWithMetadata(segments, batchSize);
   }

   @Override
   public RemoteCache<K, V> withDataFormat(DataFormat dataFormat) {
      return delegate.withDataFormat(dataFormat);
   }

   @Override
   public StreamingRemoteCache<K> streaming() {
      return delegate.streaming();
   }

   @Override
   public DataFormat getDataFormat() {
      return delegate.getDataFormat();
   }

   @Override
   public RemoteCacheClientStatisticsMXBean clientStatistics() {
      return delegate.clientStatistics();
   }

   @Override
   public ServerStatistics serverStatistics() {
      return delegate.serverStatistics();
   }

   @Override
   public boolean isTransactional() {
      return delegate.isTransactional();
   }
}
