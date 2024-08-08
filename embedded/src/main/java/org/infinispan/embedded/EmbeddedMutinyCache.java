package org.infinispan.embedded;

import java.util.Map;
import java.util.Set;

import org.infinispan.AdvancedCache;
import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryVersion;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.api.common.events.cache.CacheEntryEvent;
import org.infinispan.api.common.events.cache.CacheEntryEventType;
import org.infinispan.api.common.events.cache.CacheListenerOptions;
import org.infinispan.api.common.process.CacheEntryProcessorResult;
import org.infinispan.api.configuration.CacheConfiguration;
import org.infinispan.api.mutiny.MutinyCache;
import org.infinispan.api.mutiny.MutinyCacheEntryProcessor;
import org.infinispan.api.mutiny.MutinyContainer;
import org.infinispan.api.mutiny.MutinyQuery;
import org.infinispan.api.mutiny.MutinyStreamingCache;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 15.0
 */
public class EmbeddedMutinyCache<K, V> implements MutinyCache<K, V> {
   private final Embedded embedded;
   private final AdvancedCache<K, V> cache;

   EmbeddedMutinyCache(Embedded embedded, AdvancedCache<K, V> cache) {
      this.embedded = embedded;
      this.cache = cache;
   }

   @Override
   public String name() {
      return cache.getName();
   }

   @Override
   public Uni<CacheConfiguration> configuration() {
      return null;
   }

   @Override
   public MutinyContainer container() {
      return embedded.mutiny();
   }

   @Override
   public Uni<CacheEntry<K, V>> getEntry(K key, CacheOptions options) {
      return null;
   }

   @Override
   public Uni<CacheEntry<K, V>> putIfAbsent(K key, V value, CacheWriteOptions options) {
      return null;
   }

   @Override
   public Uni<CacheEntry<K, V>> put(K key, V value, CacheWriteOptions options) {
      return null;
   }

   @Override
   public Uni<CacheEntry<K, V>> getAndRemove(K key, CacheOptions options) {
      return null;
   }

   @Override
   public Multi<CacheEntry<K, V>> entries(CacheOptions options) {
      return null;
   }

   @Override
   public Multi<CacheEntry<K, V>> getAll(Set<K> keys, CacheOptions options) {
      return null;
   }

   @Override
   public Multi<CacheEntry<K, V>> getAll(CacheOptions options, K... keys) {
      return null;
   }

   @Override
   public Uni<Void> putAll(Multi<CacheEntry<K, V>> pairs, CacheWriteOptions options) {
      return null;
   }

   @Override
   public Uni<Void> putAll(Map<K, V> map, CacheWriteOptions options) {
      return null;
   }

   @Override
   public Uni<CacheEntry<K, V>> getOrReplaceEntry(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      return null;
   }

   @Override
   public Multi<K> removeAll(Set<K> keys, CacheWriteOptions options) {
      return null;
   }

   @Override
   public Multi<K> removeAll(Multi<K> keys, CacheWriteOptions options) {
      return null;
   }

   @Override
   public Uni<Long> estimateSize(CacheOptions options) {
      return Uni.createFrom().completionStage(cache.sizeAsync());
   }

   @Override
   public Uni<Void> clear(CacheOptions options) {
      return Uni.createFrom().completionStage(cache.clearAsync());
   }

   @Override
   public <R> MutinyQuery<K, V, R> query(String query, CacheOptions options) {
      return new EmbeddedMutinyQuery<>(cache.query(query), options);
   }

   @Override
   public Multi<CacheEntryEvent<K, V>> listen(CacheListenerOptions options, CacheEntryEventType... types) {
      return null;
   }

   @Override
   public <T> Multi<CacheEntryProcessorResult<K, T>> process(Set<K> keys, MutinyCacheEntryProcessor<K, V, T> processor, CacheOptions options) {
      return null;
   }

   @Override
   public MutinyStreamingCache<K> streaming() {
      return new EmbeddedMutinyStreamingCache<>(cache);
   }
}
