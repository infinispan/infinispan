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
import org.infinispan.api.common.process.CacheProcessor;
import org.infinispan.api.configuration.CacheConfiguration;
import org.infinispan.api.mutiny.MutinyCache;
import org.infinispan.api.mutiny.MutinyCacheEntryProcessor;
import org.infinispan.api.mutiny.MutinyQuery;
import org.infinispan.api.mutiny.MutinyStreamingCache;
import org.infinispan.hotrod.impl.cache.RemoteCache;

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
      throw new UnsupportedOperationException();
   }

   @Override
   public HotRodMutinyContainer container() {
      return hotrod.mutiny();
   }

   @Override
   public Uni<V> get(K key, CacheOptions options) {
      return Uni.createFrom().completionStage(remoteCache.get(key, options));
   }

   @Override
   public Uni<CacheEntry<K, V>> getEntry(K key, CacheOptions options) {
      return Uni.createFrom().completionStage(remoteCache.getEntry(key, options));
   }

   @Override
   public Uni<V> putIfAbsent(K key, V value, CacheWriteOptions options) {
      return Uni.createFrom().completionStage(remoteCache.putIfAbsent(key, value, options));
   }

   @Override
   public Uni<Boolean> setIfAbsent(K key, V value, CacheWriteOptions options) {
      return Uni.createFrom().completionStage(remoteCache.setIfAbsent(key, value, options));
   }

   @Override
   public Uni<V> put(K key, V value, CacheWriteOptions options) {
      return Uni.createFrom().completionStage(remoteCache.put(key, value, options));
   }

   @Override
   public Uni<Void> set(K key, V value, CacheWriteOptions options) {
      return Uni.createFrom().completionStage(remoteCache.set(key, value, options));
   }

   @Override
   public Uni<Boolean> remove(K key, CacheOptions options) {
      return Uni.createFrom().completionStage(remoteCache.remove(key, options));
   }

   @Override
   public Uni<V> getAndRemove(K key, CacheOptions options) {
      return Uni.createFrom().completionStage(remoteCache.getAndRemove(key, options));
   }

   @Override
   public Multi<K> keys() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Multi<K> keys(CacheOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Multi<CacheEntry<K, V>> entries() {
      throw new UnsupportedOperationException();
   }

   @Override
   public Multi<CacheEntry<K, V>> entries(CacheOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Multi<CacheEntry<K, V>> getAll(Set<K> keys, CacheOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Multi<CacheEntry<K, V>> getAll(CacheOptions options, K... keys) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Void> putAll(Multi<CacheEntry<K, V>> pairs, CacheWriteOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Void> putAll(Map<K, V> map, CacheWriteOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Boolean> replace(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      return Uni.createFrom().completionStage(remoteCache.replace(key, value, version, options));
   }

   @Override
   public Uni<CacheEntry<K, V>> getOrReplaceEntry(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      return Uni.createFrom().completionStage(remoteCache.getOrReplaceEntry(key, value, version, options));
   }

   @Override
   public Multi<K> removeAll(Set<K> keys, CacheOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Multi<K> removeAll(Multi<K> keys, CacheOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Multi<CacheEntry<K, V>> getAndRemoveAll(Multi<K> keys, CacheOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Uni<Long> estimateSize(CacheOptions options) {
      return Uni.createFrom().completionStage(remoteCache.estimateSize(options));
   }

   @Override
   public Uni<Void> clear(CacheOptions options) {
      return Uni.createFrom().completionStage(remoteCache.clear(options));
   }

   @Override
   public <R> MutinyQuery<K, V, R> query(String query, CacheOptions options) {
      return new HotRodMutinyQuery(query, options);
   }

   @Override
   public Multi<CacheEntryEvent<K, V>> listen(CacheListenerOptions options, CacheEntryEventType... types) {
      throw new UnsupportedOperationException();
   }

   @Override
   public <T> Multi<CacheEntryProcessorResult<K, T>> process(Set<K> keys, MutinyCacheEntryProcessor<K, V, T> processor, CacheOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public <T> Multi<CacheEntryProcessorResult<K, T>> processAll(CacheProcessor task, CacheOptions options) {
      throw new UnsupportedOperationException();
   }

   @Override
   public MutinyStreamingCache<K> streaming() {
      return new HotRodMutinyStreamingCache(hotrod, remoteCache);
   }
}
