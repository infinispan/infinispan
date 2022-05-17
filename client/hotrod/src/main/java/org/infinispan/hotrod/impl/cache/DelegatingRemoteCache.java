package org.infinispan.hotrod.impl.cache;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import javax.transaction.TransactionManager;

import org.infinispan.api.async.AsyncCacheEntryProcessor;
import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryVersion;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.api.common.events.cache.CacheEntryEvent;
import org.infinispan.api.common.events.cache.CacheEntryEventType;
import org.infinispan.api.common.events.cache.CacheListenerOptions;
import org.infinispan.api.common.process.CacheEntryProcessorResult;
import org.infinispan.api.common.process.CacheProcessorOptions;
import org.infinispan.api.configuration.CacheConfiguration;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.hotrod.impl.DataFormat;
import org.infinispan.hotrod.impl.HotRodTransport;
import org.infinispan.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.hotrod.impl.operations.RetryAwareCompletionStage;
import org.reactivestreams.Publisher;

/**
 * Delegates all invocations to the provided underlying {@link RemoteCache} but provides extensibility to intercept when
 * a method is invoked.
 *
 * @param <K> key type
 * @param <V> value type
 */
public abstract class DelegatingRemoteCache<K, V> implements RemoteCache<K, V> {
   protected final RemoteCache<K, V> delegate;

   protected DelegatingRemoteCache(RemoteCache<K, V> delegate) {
      this.delegate = delegate;
   }

   abstract <Key, Value> RemoteCache<Key, Value> newDelegatingCache(RemoteCache<Key, Value> innerCache);

   @Override
   public CompletionStage<CacheConfiguration> configuration() {
      return delegate.configuration();
   }

   @Override
   public HotRodTransport getHotRodTransport() {
      return delegate.getHotRodTransport();
   }

   @Override
   public CacheOperationsFactory getOperationsFactory() {
      return delegate.getOperationsFactory();
   }

   @Override
   public CompletionStage<V> get(K key, CacheOptions options) {
      return delegate.get(key, options);
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> getEntry(K key, CacheOptions options) {
      return delegate.getEntry(key, options);
   }

   @Override
   public RetryAwareCompletionStage<CacheEntry<K, V>> getEntry(K key, CacheOptions options, SocketAddress listenerAddress) {
      return delegate.getEntry(key, options, listenerAddress);
   }

   @Override
   public CompletionStage<V> putIfAbsent(K key, V value, CacheWriteOptions options) {
      return delegate.putIfAbsent(key, value, options);
   }

   @Override
   public CompletionStage<Boolean> setIfAbsent(K key, V value, CacheWriteOptions options) {
      return delegate.setIfAbsent(key, value, options);
   }

   @Override
   public CompletionStage<V> put(K key, V value, CacheWriteOptions options) {
      return delegate.put(key, value, options);
   }

   @Override
   public CompletionStage<Void> set(K key, V value, CacheWriteOptions options) {
      return delegate.set(key, value, options);
   }

   @Override
   public CompletionStage<Boolean> replace(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      return delegate.replace(key, value, version, options);
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> getOrReplaceEntry(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      return delegate.getOrReplaceEntry(key, value, version, options);
   }

   @Override
   public CompletionStage<Boolean> remove(K key, CacheOptions options) {
      return delegate.remove(key, options);
   }

   @Override
   public CompletionStage<Boolean> remove(K key, CacheEntryVersion version, CacheOptions options) {
      return delegate.remove(key, version, options);
   }

   @Override
   public CompletionStage<V> getAndRemove(K key, CacheOptions options) {
      return delegate.getAndRemove(key, options);
   }

   @Override
   public Flow.Publisher<K> keys(CacheOptions options) {
      return delegate.keys(options);
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> entries(CacheOptions options) {
      return delegate.entries(options);
   }

   @Override
   public CompletionStage<Void> putAll(Map<K, V> entries, CacheWriteOptions options) {
      return delegate.putAll(entries, options);
   }

   @Override
   public CompletionStage<Void> putAll(Flow.Publisher<CacheEntry<K, V>> entries, CacheWriteOptions options) {
      return delegate.putAll(entries, options);
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAll(Set<K> keys, CacheOptions options) {
      return delegate.getAll(keys, options);
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAll(CacheOptions options, K[] keys) {
      return delegate.getAll(options, keys);
   }

   @Override
   public Flow.Publisher<K> removeAll(Set<K> keys, CacheWriteOptions options) {
      return delegate.removeAll(keys, options);
   }

   @Override
   public Flow.Publisher<K> removeAll(Flow.Publisher<K> keys, CacheWriteOptions options) {
      return delegate.removeAll(keys, options);
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Set<K> keys, CacheWriteOptions options) {
      return delegate.getAndRemoveAll(keys, options);
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Flow.Publisher<K> keys, CacheWriteOptions options) {
      return delegate.getAndRemoveAll(keys, options);
   }

   @Override
   public CompletionStage<Long> estimateSize(CacheOptions options) {
      return delegate.estimateSize(options);
   }

   @Override
   public CompletionStage<Void> clear(CacheOptions options) {
      return delegate.clear(options);
   }

   @Override
   public CloseableIterator<CacheEntry<Object, Object>> retrieveEntries(String filterConverterFactory, Set<Integer> segments, int batchSize) {
      return delegate.retrieveEntries(filterConverterFactory, segments, batchSize);
   }

   @Override
   public CloseableIterator<CacheEntry<Object, Object>> retrieveEntries(String filterConverterFactory, Object[] filterConverterParams, Set<Integer> segments, int batchSize) {
      return delegate.retrieveEntries(filterConverterFactory, filterConverterParams, segments, batchSize);
   }

   @Override
   public Flow.Publisher<CacheEntryEvent<K, V>> listen(CacheListenerOptions options, CacheEntryEventType[] types) {
      return delegate.listen(options, types);
   }

   @Override
   public <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> process(Set<K> keys, AsyncCacheEntryProcessor<K, V, T> task, CacheOptions options) {
      return delegate.process(keys, task, options);
   }

   @Override
   public <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> processAll(AsyncCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options) {
      return delegate.processAll(processor, options);
   }

   @Override
   public <T, U> RemoteCache<T, U> withDataFormat(DataFormat newDataFormat) {
      return delegate.withDataFormat(newDataFormat);
   }

   @Override
   public K keyAsObjectIfNeeded(Object key) {
      return delegate.keyAsObjectIfNeeded(key);
   }

   @Override
   public byte[] keyToBytes(Object o) {
      return delegate.keyToBytes(o);
   }

   @Override
   public byte[] valueToBytes(Object o) {
      return delegate.valueToBytes(o);
   }

   @Override
   public void resolveStorage(boolean objectStorage) {
      delegate.resolveStorage(objectStorage);
   }

   @Override
   public CompletionStage<Void> updateBloomFilter() {
      return delegate.updateBloomFilter();
   }

   @Override
   public SocketAddress addNearCacheListener(Object listener, int bloomFilterBits) {
      return delegate.addNearCacheListener(listener, bloomFilterBits);
   }

   @Override
   public String getName() {
      return delegate.getName();
   }

   @Override
   public DataFormat getDataFormat() {
      return delegate.getDataFormat();
   }

   @Override
   public <E> Publisher<CacheEntry<K, E>> publishEntries(String filterConverterFactory, Object[] filterConverterParams, Set<Integer> segments, int batchSize) {
      return delegate.publishEntries(filterConverterFactory, filterConverterParams, segments, batchSize);
   }

   @Override
   public CloseableIterator<CacheEntry<Object, Object>> retrieveEntriesByQuery(RemoteQuery query, Set<Integer> segments, int batchSize) {
      return delegate.retrieveEntriesByQuery(query, segments, batchSize);
   }

   @Override
   public <E> Publisher<CacheEntry<K, E>> publishEntriesByQuery(RemoteQuery query, Set<Integer> segments, int batchSize) {
      return delegate.publishEntriesByQuery(query, segments, batchSize);
   }

   @Override
   public CloseableIterator<CacheEntry<Object, Object>> retrieveEntriesWithMetadata(Set<Integer> segments, int batchSize) {
      return delegate.retrieveEntriesWithMetadata(segments, batchSize);
   }

   @Override
   public Publisher<CacheEntry<K, V>> publishEntriesWithMetadata(Set<Integer> segments, int batchSize) {
      return delegate.publishEntriesWithMetadata(segments, batchSize);
   }

   @Override
   public TransactionManager getTransactionManager() {
      return delegate.getTransactionManager();
   }

   @Override
   public boolean isTransactional() {
      return delegate.isTransactional();
   }

   @Override
   public void close() throws Exception {
      delegate.close();
   }
}
