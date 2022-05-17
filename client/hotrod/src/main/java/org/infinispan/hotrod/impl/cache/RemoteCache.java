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
 * @since 14.0
 **/
public interface RemoteCache<K, V> extends AutoCloseable {
   CompletionStage<CacheConfiguration> configuration();

   HotRodTransport getHotRodTransport();

   CacheOperationsFactory getOperationsFactory();

   CompletionStage<V> get(K key, CacheOptions options);

   K keyAsObjectIfNeeded(Object key);

   byte[] keyToBytes(Object o);

   byte[] valueToBytes(Object o);

   CompletionStage<CacheEntry<K, V>> getEntry(K key, CacheOptions options);

   RetryAwareCompletionStage<CacheEntry<K, V>> getEntry(K key, CacheOptions options, SocketAddress listenerAddress);

   CompletionStage<V> putIfAbsent(K key, V value, CacheWriteOptions options);

   CompletionStage<Boolean> setIfAbsent(K key, V value, CacheWriteOptions options);

   CompletionStage<V> put(K key, V value, CacheWriteOptions options);

   CompletionStage<Void> set(K key, V value, CacheWriteOptions options);

   CompletionStage<Boolean> replace(K key, V value, CacheEntryVersion version, CacheWriteOptions options);

   CompletionStage<CacheEntry<K,V>> getOrReplaceEntry(K key, V value, CacheEntryVersion version, CacheWriteOptions options);

   CompletionStage<Boolean> remove(K key, CacheOptions options);

   CompletionStage<Boolean> remove(K key, CacheEntryVersion version, CacheOptions options);

   CompletionStage<V> getAndRemove(K key, CacheOptions options);

   Flow.Publisher<K> keys(CacheOptions options);

   Flow.Publisher<CacheEntry<K,V>> entries(CacheOptions options);

   CompletionStage<Void> putAll(Map<K, V> entries, CacheWriteOptions options);

   CompletionStage<Void> putAll(Flow.Publisher<CacheEntry<K, V>> entries, CacheWriteOptions options);

   Flow.Publisher<CacheEntry<K, V>> getAll(Set<K> keys, CacheOptions options);

   Flow.Publisher<CacheEntry<K, V>> getAll(CacheOptions options, K[] keys);

   Flow.Publisher<K> removeAll(Set<K> keys, CacheWriteOptions options);

   Flow.Publisher<K> removeAll(Flow.Publisher<K> keys, CacheWriteOptions options);

   Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Set<K> keys, CacheWriteOptions options);

   Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Flow.Publisher<K> keys, CacheWriteOptions options);

   CompletionStage<Long> estimateSize(CacheOptions options);

   CompletionStage<Void> clear(CacheOptions options);

   Flow.Publisher<CacheEntryEvent<K, V>> listen(CacheListenerOptions options, CacheEntryEventType[] types);

   <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> process(Set<K> keys, AsyncCacheEntryProcessor<K, V, T> task, CacheOptions options);

   <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> processAll(AsyncCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options);

   default CloseableIterator<CacheEntry<Object, Object>> retrieveEntries(String filterConverterFactory, Set<Integer> segments, int batchSize) {
      return retrieveEntries(filterConverterFactory, null, segments, batchSize);
   }

   CloseableIterator<CacheEntry<Object, Object>> retrieveEntries(String filterConverterFactory, Object[] filterConverterParams, Set<Integer> segments, int batchSize);

   <T, U> RemoteCache<T, U> withDataFormat(DataFormat newDataFormat);

   void resolveStorage(boolean objectStorage);

   CompletionStage<Void> updateBloomFilter();

   SocketAddress addNearCacheListener(Object listener, int bloomFilterBits);

   String getName();

   DataFormat getDataFormat();

   <E> Publisher<CacheEntry<K, E>> publishEntries(String filterConverterFactory, Object[] filterConverterParams, Set<Integer> segments, int batchSize);

   CloseableIterator<CacheEntry<Object, Object>> retrieveEntriesByQuery(RemoteQuery query, Set<Integer> segments, int batchSize);

   <E> Publisher<CacheEntry<K, E>> publishEntriesByQuery(RemoteQuery query, Set<Integer> segments, int batchSize);

   CloseableIterator<CacheEntry<Object, Object>> retrieveEntriesWithMetadata(Set<Integer> segments, int batchSize);

   Publisher<CacheEntry<K, V>> publishEntriesWithMetadata(Set<Integer> segments, int batchSize);

   TransactionManager getTransactionManager();

   boolean isTransactional();
}
