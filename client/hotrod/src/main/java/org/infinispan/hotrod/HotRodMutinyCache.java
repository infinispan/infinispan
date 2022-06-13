package org.infinispan.hotrod;

import java.util.Map;
import java.util.Set;

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
import org.infinispan.api.mutiny.MutinyQuery;
import org.infinispan.api.mutiny.MutinyStreamingCache;
import org.infinispan.hotrod.impl.cache.RemoteCache;
import org.reactivestreams.FlowAdapters;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;

/**
 * @since 14.0
 **/
public class HotRodMutinyCache<K, V> implements MutinyCache<K, V> {
   private final HotRod hotrod;
   private final RemoteCache<K, V> remoteCache;

   HotRodMutinyCache(HotRod hotrod, RemoteCache<K, V> remoteCache) {
      this.hotrod = hotrod;
      this.remoteCache = remoteCache;
   }

   @Override
   public String name() {
      return remoteCache.getName();
   }

   @Override
   public Uni<CacheConfiguration> configuration() {
      return Uni.createFrom().completionStage(remoteCache.configuration());
   }

   @Override
   public HotRodMutinyContainer container() {
      return hotrod.mutiny();
   }

   @Override
   public Uni<V> get(K key, CacheOptions options) {
      return Uni.createFrom().completionStage(() -> remoteCache.get(key, options));
   }

   @Override
   public Uni<CacheEntry<K, V>> getEntry(K key, CacheOptions options) {
      return Uni.createFrom().completionStage(() -> remoteCache.getEntry(key, options));
   }

   @Override
   public Uni<CacheEntry<K, V>> putIfAbsent(K key, V value, CacheWriteOptions options) {
      return Uni.createFrom().completionStage(() -> remoteCache.putIfAbsent(key, value, options));
   }

   @Override
   public Uni<Boolean> setIfAbsent(K key, V value, CacheWriteOptions options) {
      return Uni.createFrom().completionStage(() -> remoteCache.setIfAbsent(key, value, options));
   }

   @Override
   public Uni<CacheEntry<K, V>> put(K key, V value, CacheWriteOptions options) {
      return Uni.createFrom().completionStage(() -> remoteCache.put(key, value, options));
   }

   @Override
   public Uni<Void> set(K key, V value, CacheWriteOptions options) {
      return Uni.createFrom().completionStage(() -> remoteCache.set(key, value, options));
   }

   @Override
   public Uni<Boolean> remove(K key, CacheOptions options) {
      return Uni.createFrom().completionStage(() -> remoteCache.remove(key, options));
   }

   @Override
   public Uni<CacheEntry<K, V>> getAndRemove(K key, CacheOptions options) {
      return Uni.createFrom().completionStage(() -> remoteCache.getAndRemove(key, options));
   }

   @Override
   public Multi<K> keys(CacheOptions options) {
      return Multi.createFrom().publisher(FlowAdapters.toPublisher(remoteCache.keys(options)));
   }

   @Override
   public Multi<CacheEntry<K, V>> entries(CacheOptions options) {
      return Multi.createFrom().publisher(FlowAdapters.toPublisher(remoteCache.entries(options)));
   }

   @Override
   public Multi<CacheEntry<K, V>> getAll(Set<K> keys, CacheOptions options) {
      return Multi.createFrom().publisher(FlowAdapters.toPublisher(remoteCache.getAll(keys, options)));
   }

   @Override
   public Multi<CacheEntry<K, V>> getAll(CacheOptions options, K... keys) {
      return Multi.createFrom().publisher(FlowAdapters.toPublisher(remoteCache.getAll(options, keys)));
   }

   @Override
   public Uni<Void> putAll(Multi<CacheEntry<K, V>> entries, CacheWriteOptions options) {
      return Uni.createFrom().completionStage(() -> remoteCache.putAll(FlowAdapters.toFlowPublisher(entries.convert().toPublisher()), options));
   }

   @Override
   public Uni<Void> putAll(Map<K, V> map, CacheWriteOptions options) {
      return Uni.createFrom().completionStage(() -> remoteCache.putAll(map, options));
   }

   @Override
   public Uni<Boolean> replace(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      return Uni.createFrom().completionStage(() -> remoteCache.replace(key, value, version, options));
   }

   @Override
   public Uni<CacheEntry<K, V>> getOrReplaceEntry(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      return Uni.createFrom().completionStage(() -> remoteCache.getOrReplaceEntry(key, value, version, options));
   }

   @Override
   public Multi<K> removeAll(Set<K> keys, CacheWriteOptions options) {
      return Multi.createFrom().publisher(FlowAdapters.toPublisher(remoteCache.removeAll(keys, options)));
   }

   @Override
   public Multi<K> removeAll(Multi<K> keys, CacheWriteOptions options) {
      return Multi.createFrom().publisher(FlowAdapters.toPublisher(remoteCache.removeAll(
            FlowAdapters.toFlowPublisher(keys.convert().toPublisher()), options)));
   }

   @Override
   public Multi<CacheEntry<K, V>> getAndRemoveAll(Multi<K> keys, CacheWriteOptions options) {
      return Multi.createFrom().publisher(FlowAdapters.toPublisher(remoteCache.getAndRemoveAll(
            FlowAdapters.toFlowPublisher(keys.convert().toPublisher()), options)));
   }

   @Override
   public Uni<Long> estimateSize(CacheOptions options) {
      return Uni.createFrom().completionStage(() -> remoteCache.estimateSize(options));
   }

   @Override
   public Uni<Void> clear(CacheOptions options) {
      return Uni.createFrom().completionStage(() -> remoteCache.clear(options));
   }

   @Override
   public <R> MutinyQuery<K, V, R> query(String query, CacheOptions options) {
      return new HotRodMutinyQuery(query, options);
   }

   @Override
   public Multi<CacheEntryEvent<K, V>> listen(CacheListenerOptions options, CacheEntryEventType... types) {
      return Multi.createFrom().publisher(FlowAdapters.toPublisher(remoteCache.listen(options, types)));
   }

   @Override
   public <T> Multi<CacheEntryProcessorResult<K, T>> process(Set<K> keys, MutinyCacheEntryProcessor<K, V, T> processor, CacheOptions options) {
      return Multi.createFrom().publisher(FlowAdapters.toPublisher(
            remoteCache.process(keys, new MutinyToAsyncCacheEntryProcessor<>(processor), options)));
   }

   @Override
   public MutinyStreamingCache<K> streaming() {
      return new HotRodMutinyStreamingCache(hotrod, remoteCache);
   }
}
