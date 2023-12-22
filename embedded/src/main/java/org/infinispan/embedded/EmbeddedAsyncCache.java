package org.infinispan.embedded;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

import org.infinispan.AdvancedCache;
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

/**
 * @since 15.0
 */
public class EmbeddedAsyncCache<K, V> implements AsyncCache<K, V> {
   private final Embedded embedded;
   private final AdvancedCache<K, V> cache;

   EmbeddedAsyncCache(Embedded embedded, AdvancedCache<K, V> cache) {
      this.embedded = embedded;
      this.cache = cache;
   }

   @Override
   public String name() {
      return cache.getName();
   }

   @Override
   public CompletionStage<CacheConfiguration> configuration() {
      return null;
   }

   @Override
   public AsyncContainer container() {
      return embedded.async();
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> getEntry(K key, CacheOptions options) {
      return null;
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> putIfAbsent(K key, V value, CacheWriteOptions options) {
      return null;
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> put(K key, V value, CacheWriteOptions options) {
      return null;
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> getOrReplaceEntry(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      return null;
   }

   @Override
   public CompletionStage<Boolean> remove(K key, CacheOptions options) {
      return null;
   }

   @Override
   public CompletionStage<Boolean> remove(K key, CacheEntryVersion version, CacheOptions options) {
      return null;
   }

   @Override
   public CompletionStage<CacheEntry<K, V>> getAndRemove(K key, CacheOptions options) {
      return null;
   }

   @Override
   public Flow.Publisher<K> keys(CacheOptions options) {
      return null;
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> entries(CacheOptions options) {
      return null;
   }

   @Override
   public CompletionStage<Void> putAll(Map<K, V> entries, CacheWriteOptions options) {
      return null;
   }

   @Override
   public CompletionStage<Void> putAll(Flow.Publisher<CacheEntry<K, V>> entries, CacheWriteOptions options) {
      return null;
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAll(Set<K> keys, CacheOptions options) {
      return null;
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAll(CacheOptions options, K... keys) {
      return null;
   }

   @Override
   public Flow.Publisher<K> removeAll(Set<K> keys, CacheWriteOptions options) {
      return null;
   }

   @Override
   public Flow.Publisher<K> removeAll(Flow.Publisher<K> keys, CacheWriteOptions options) {
      return null;
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Set<K> keys, CacheWriteOptions options) {
      return null;
   }

   @Override
   public Flow.Publisher<CacheEntry<K, V>> getAndRemoveAll(Flow.Publisher<K> keys, CacheWriteOptions options) {
      return null;
   }

   @Override
   public CompletionStage<Long> estimateSize(CacheOptions options) {
      return null;
   }

   @Override
   public CompletionStage<Void> clear(CacheOptions options) {
      return cache.clearAsync();
   }

   @Override
   public <R> AsyncQuery<K, V, R> query(String query, CacheOptions options) {
      return new EmbeddedAsyncQuery<>(cache.query(query), options);
   }

   @Override
   public Flow.Publisher<CacheEntryEvent<K, V>> listen(CacheListenerOptions options, CacheEntryEventType... types) {
      return null;
   }

   @Override
   public <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> process(Set<K> keys, AsyncCacheEntryProcessor<K, V, T> task, CacheOptions options) {
      return null;
   }

   @Override
   public <T> Flow.Publisher<CacheEntryProcessorResult<K, T>> processAll(AsyncCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options) {
      return null;
   }

   @Override
   public AsyncStreamingCache<K> streaming() {
      return new EmbeddedAsyncStreamingCache<>(cache);
   }
}
