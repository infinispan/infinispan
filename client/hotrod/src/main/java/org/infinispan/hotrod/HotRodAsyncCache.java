package org.infinispan.hotrod;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.api.async.AsyncCache;
import org.infinispan.api.async.AsyncCacheEntryProcessor;
import org.infinispan.api.async.AsyncContainer;
import org.infinispan.api.async.AsyncQuery;
import org.infinispan.api.async.AsyncStreamingCache;
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
import org.infinispan.hotrod.impl.cache.RemoteCache;

/**
 * @since 14.0
 **/
public class HotRodAsyncCache<K, V> implements AsyncCache<K, V> {
   private final HotRod hotrod;
   private final RemoteCache<K, V> remoteCache;

   HotRodAsyncCache(HotRod hotrod, RemoteCache<K, V> remoteCache) {
      this.hotrod = hotrod;
      this.remoteCache = remoteCache;
   }

   @Override
   public String name() {
      return remoteCache.getName();
   }

   @Override
   public CompletionStage<CacheConfiguration> configuration() {
      return remoteCache.configuration();
   }

   @Override
   public AsyncContainer container() {
      return hotrod.async();
   }

   @Override
   public CompletionStage<V> get(K key, CacheOptions options) {
      return remoteCache.get(key, options);
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> getEntry(K key, CacheOptions options) {
      return remoteCache.getEntry(key, options);
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> putIfAbsent(K key, V value, CacheWriteOptions options) {
      return remoteCache.putIfAbsent(key, value, options);
   }

   @Override
   public CompletionStage<Boolean> setIfAbsent(K key, V value, CacheWriteOptions options) {
      return remoteCache.setIfAbsent(key, value, options);
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> put(K key, V value, CacheWriteOptions options) {
      return remoteCache.put(key, value, options);
   }

   @Override
   public CompletionStage<Void> set(K key, V value, CacheWriteOptions options) {
      return remoteCache.set(key, value, options);
   }

   @Override
   public CompletionStage<Boolean> replace(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      return remoteCache.replace(key, value, version, options);
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> getOrReplaceEntry(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      return remoteCache.getOrReplaceEntry(key, value, version, options);
   }

   @Override
   public CompletionStage<Boolean> remove(K key, CacheOptions options) {
      return remoteCache.remove(key, options);
   }

   @Override
   public CompletionStage<Boolean> remove(K key, CacheEntryVersion version, CacheOptions options) {
      return remoteCache.remove(key, version, options);
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> getAndRemove(K key, CacheOptions options) {
      return remoteCache.getAndRemove(key, options);
   }

   @Override
   public Flow.Publisher<K> keys(CacheOptions options) {
      return remoteCache.keys(options);
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> entries(CacheOptions options) {
      return remoteCache.entries(options);
   }

   @Override
   public CompletionStage<Void> putAll(Map<K, V> entries, CacheWriteOptions options) {
      return remoteCache.putAll(entries, options);
   }

   @Override
   public CompletionStage<Void> putAll(Flow.Publisher<CacheEntry<K, V>> entries, CacheWriteOptions options) {
      return remoteCache.putAll(entries, options);
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAll(Set<K> keys, CacheOptions options) {
      return remoteCache.getAll(keys, options);
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAll(CacheOptions options, K... keys) {
      return remoteCache.getAll(options, keys);
   }

   @Override
   public Flow.Publisher<K> removeAll(Set<K> keys, CacheWriteOptions options) {
      return remoteCache.removeAll(keys, options);
   }

   @Override
   public Flow.Publisher<K> removeAll(Flow.Publisher<K> keys, CacheWriteOptions options) {
      return remoteCache.removeAll(keys, options);
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Set<K> keys, CacheWriteOptions options) {
      return remoteCache.getAndRemoveAll(keys, options);
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Flow.Publisher<K> keys, CacheWriteOptions options) {
      return remoteCache.getAndRemoveAll(keys, options);
   }

   @Override
   public CompletionStage<Long> estimateSize(CacheOptions options) {
      return remoteCache.estimateSize(options);
   }

   @Override
   public CompletionStage<Void> clear(CacheOptions options) {
      return remoteCache.clear(options);
   }

   @Override
   public <R> AsyncQuery<K, V, R> query(String query, CacheOptions options) {
      return new HotRodAsyncQuery(query, options);
   }

   @Override
   public Flow.Publisher<CacheEntryEvent<K, V>> listen(CacheListenerOptions options, CacheEntryEventType... types) {
      return remoteCache.listen(options, types);
   }

   @Override
   public <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> process(Set<K> keys, AsyncCacheEntryProcessor<K, V, T> processor, CacheOptions options) {
      return remoteCache.process(keys, processor, options);
   }

   @Override
   public <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> processAll(AsyncCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options) {
      return remoteCache.processAll(processor, options);
   }

   @Override
   public AsyncStreamingCache<K> streaming() {
      return new HotRodAsyncStreamingCache(hotrod, remoteCache);
   }
}
