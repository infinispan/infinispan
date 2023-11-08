package org.infinispan.client.hotrod.impl;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;

import javax.management.ObjectName;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCacheContainer;
import org.infinispan.client.hotrod.ServerStatistics;
import org.infinispan.client.hotrod.StreamingRemoteCache;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.operations.PingResponse;
import org.infinispan.client.hotrod.impl.operations.RetryAwareCompletionStage;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorCollection;
import org.infinispan.commons.util.CloseableIteratorSet;
import org.infinispan.commons.util.IntSet;
import org.infinispan.query.dsl.Query;
import org.reactivestreams.Publisher;

/**
 * Delegates all invocations to the provided underlying {@link InternalRemoteCache} but provides extensibility to intercept
 * when a method is invoked. Currently all methods are supported except for iterators produced from the
 * {@link #keyIterator(IntSet)} and {@link #entryIterator(IntSet)} which are known to invoke back into the delegate cache.
 * @param <K> key type
 * @param <V> value type
 */
public abstract class DelegatingRemoteCache<K, V> extends RemoteCacheSupport<K, V> implements InternalRemoteCache<K, V> {
   protected final InternalRemoteCache<K, V> delegate;

   protected DelegatingRemoteCache(InternalRemoteCache<K, V> delegate) {
      this.delegate = delegate;
   }

   abstract <Key, Value> InternalRemoteCache<Key, Value> newDelegatingCache(InternalRemoteCache<Key, Value> innerCache);

   @Override
   public CompletableFuture<Void> putAllAsync(Map<? extends K, ? extends V> data, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return delegate.putAllAsync(data, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<Void> clearAsync() {
      return delegate.clearAsync();
   }

   @Override
   public ClientStatistics clientStatistics() {
      return delegate.clientStatistics();
   }

   @Override
   public ServerStatistics serverStatistics() {
      return delegate.serverStatistics();
   }

    @Override
    public CompletionStage<ServerStatistics> serverStatisticsAsync() {
        return delegate.serverStatisticsAsync();
    }

    @Override
   public InternalRemoteCache<K, V> withFlags(Flag... flags) {
      InternalRemoteCache<K, V> newCache = delegate.withFlags(flags);
      if (newCache != delegate) {
         return newDelegatingCache(newCache);
      }
      return this;
   }

   @Override
   public RemoteCacheContainer getRemoteCacheContainer() {
      return delegate.getRemoteCacheContainer();
   }

   @Override
   public CompletableFuture<Map<K, V>> getAllAsync(Set<?> keys) {
      return delegate.getAllAsync(keys);
   }

   @Override
   public String getProtocolVersion() {
      return delegate.getProtocolVersion();
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
   public void removeClientListener(Object listener) {
      delegate.removeClientListener(listener);
   }

   @Override
   public SocketAddress addNearCacheListener(Object listener, int bloomBits) {
      return delegate.addNearCacheListener(listener, bloomBits);
   }

   @Override
   public Set<Object> getListeners() {
      return delegate.getListeners();
   }

   @Override
   public <T> T execute(String taskName, Map<String, ?> params) {
      return delegate.execute(taskName, params);
   }

   @Override
   public CacheTopologyInfo getCacheTopologyInfo() {
      return delegate.getCacheTopologyInfo();
   }

   @Override
   public StreamingRemoteCache<K> streaming() {
      return delegate.streaming();
   }

   @Override
   public <T, U> InternalRemoteCache<T, U> withDataFormat(DataFormat dataFormat) {
      InternalRemoteCache<T, U> newCache = delegate.withDataFormat(dataFormat);
      if (newCache != delegate) {
         return newDelegatingCache(newCache);
      }
      //noinspection unchecked
      return (InternalRemoteCache<T, U>) this;
   }

   @Override
   public DataFormat getDataFormat() {
      return delegate.getDataFormat();
   }

   @Override
   public boolean isTransactional() {
      return delegate.isTransactional();
   }

   @Override
   public CompletableFuture<V> putIfAbsentAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return delegate.putIfAbsentAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<Boolean> replaceAsync(K key, V oldValue, V newValue, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return delegate.replaceAsync(key, oldValue, newValue, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> replaceAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return delegate.replaceAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> getAsync(K key) {
      return delegate.getAsync(key);
   }

   @Override
   public CompletableFuture<MetadataValue<V>> getWithMetadataAsync(K key) {
      return delegate.getWithMetadataAsync(key);
   }

   @Override
   public RetryAwareCompletionStage<MetadataValue<V>> getWithMetadataAsync(K key, SocketAddress preferredAddres) {
      return delegate.getWithMetadataAsync(key, preferredAddres);
   }

   @Override
   public boolean isEmpty() {
      return delegate.isEmpty();
   }

   @Override
   public boolean containsValue(Object value) {
      return delegate.containsValue(value);
   }

   @Override
   public CloseableIteratorSet<K> keySet(IntSet segments) {
      return new RemoteCacheKeySet<>(this, segments);
   }

   @Override
   public CloseableIteratorCollection<V> values(IntSet segments) {
      return new RemoteCacheValuesCollection<>(this, segments);
   }

   @Override
   public CloseableIteratorSet<Entry<K, V>> entrySet(IntSet segments) {
      return new RemoteCacheEntrySet<>(this, segments);
   }

   @Override
   public CompletableFuture<Boolean> containsKeyAsync(K key) {
      return delegate.containsKeyAsync(key);
   }

   @Override
   public CompletableFuture<V> putAsync(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return delegate.putAsync(key, value, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<Boolean> replaceWithVersionAsync(K key, V newValue, long version, long lifespanSeconds, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      return delegate.replaceWithVersionAsync(key, newValue, version, lifespanSeconds, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
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
   public CompletableFuture<V> removeAsync(Object key) {
      return delegate.removeAsync(key);
   }

   @Override
   public CompletableFuture<Boolean> removeAsync(Object key, Object value) {
      return delegate.removeAsync(key, value);
   }

   @Override
   public CompletableFuture<Boolean> removeWithVersionAsync(K key, long version) {
      return delegate.removeWithVersionAsync(key, version);
   }

   @Override
   public CompletableFuture<V> mergeAsync(K key, V value, BiFunction<? super V, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return delegate.mergeAsync(key, value, remappingFunction, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit);
   }

   @Override
   public CompletableFuture<V> computeAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return delegate.computeAsync(key, remappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> computeIfAbsentAsync(K key, Function<? super K, ? extends V> mappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return delegate.computeIfAbsentAsync(key, mappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public CompletableFuture<V> computeIfPresentAsync(K key, BiFunction<? super K, ? super V, ? extends V> remappingFunction, long lifespan, TimeUnit lifespanUnit, long maxIdle, TimeUnit maxIdleUnit) {
      return delegate.computeIfPresentAsync(key, remappingFunction, lifespan, lifespanUnit, maxIdle, maxIdleUnit);
   }

   @Override
   public void replaceAll(BiFunction<? super K, ? super V, ? extends V> function) {
      delegate.replaceAll(function);
   }

   @Override
   public CompletableFuture<Long> sizeAsync() {
      return delegate.sizeAsync();
   }

   @Override
   public String getName() {
      return delegate.getName();
   }

   @Override
   public String getVersion() {
      return delegate.getVersion();
   }

   @Override
   public void start() {
      delegate.start();
   }

   @Override
   public void stop() {
      delegate.stop();
   }

   @Override
   public CloseableIterator<K> keyIterator(IntSet segments) {
      return delegate.keyIterator(segments);
   }

   @Override
   public CloseableIterator<Entry<K, V>> entryIterator(IntSet segments) {
      return delegate.entryIterator(segments);
   }

   @Override
   public boolean hasForceReturnFlag() {
      return delegate.hasForceReturnFlag();
   }

   @Override
   public void resolveStorage(boolean objectStorage) {
      delegate.resolveStorage(objectStorage);
   }

   @Override
   public byte[] keyToBytes(Object o) {
      return delegate.keyToBytes(o);
   }

   @Override
   public void init(OperationsFactory operationsFactory, Configuration configuration, ObjectName jmxParent) {
      delegate.init(operationsFactory, configuration, jmxParent);
   }

   @Override
   public void init(OperationsFactory operationsFactory, Configuration configuration) {
      delegate.init(operationsFactory, configuration);
   }

   @Override
   public OperationsFactory getOperationsFactory() {
      return delegate.getOperationsFactory();
   }

   @Override
   public boolean isObjectStorage() {
      return delegate.isObjectStorage();
   }

   @Override
   public CompletionStage<PingResponse> ping() {
      return delegate.ping();
   }

   @Override
   public CompletionStage<Void> updateBloomFilter() {
      return delegate.updateBloomFilter();
   }
}
