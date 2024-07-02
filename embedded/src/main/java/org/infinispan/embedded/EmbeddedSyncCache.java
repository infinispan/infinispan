package org.infinispan.embedded;

import java.util.Map;
import java.util.Set;

import org.infinispan.AdvancedCache;
import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheEntryVersion;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.CacheWriteOptions;
import org.infinispan.api.common.CloseableIterable;
import org.infinispan.api.common.process.CacheEntryProcessorResult;
import org.infinispan.api.common.process.CacheProcessorOptions;
import org.infinispan.api.configuration.CacheConfiguration;
import org.infinispan.api.sync.SyncCache;
import org.infinispan.api.sync.SyncCacheEntryProcessor;
import org.infinispan.api.sync.SyncContainer;
import org.infinispan.api.sync.SyncQuery;
import org.infinispan.api.sync.SyncStreamingCache;
import org.infinispan.api.sync.events.cache.SyncCacheEntryListener;
import org.infinispan.embedded.impl.EmbeddedCacheEntry;

/**
 * @since 15.0
 */
public class EmbeddedSyncCache<K, V> implements SyncCache<K, V> {
   private final Embedded embedded;
   private final AdvancedCache<K, V> cache;

   EmbeddedSyncCache(Embedded embedded, AdvancedCache<K, V> cache) {
      this.embedded = embedded;
      this.cache = cache;
   }

   @Override
   public String name() {
      return cache.getName();
   }

   @Override
   public CacheConfiguration configuration() {
      return null;
   }

   @Override
   public SyncContainer container() {
      return embedded.sync();
   }

   @Override
   public CacheEntry<K, V> getEntry(K key, CacheOptions options) {
      return new EmbeddedCacheEntry<>(cache.getCacheEntry(key));
   }

   @Override
   public CacheEntry<K, V> put(K key, V value, CacheWriteOptions options) {
      return null;
   }

   @Override
   public CacheEntry<K, V> putIfAbsent(K key, V value, CacheWriteOptions options) {
      return null;
   }

   @Override
   public CacheEntry<K, V> getOrReplaceEntry(K key, V value, CacheEntryVersion version, CacheWriteOptions options) {
      return null;
   }

   @Override
   public boolean remove(K key, CacheEntryVersion version, CacheOptions options) {
      return false;
   }

   @Override
   public CacheEntry<K, V> getAndRemove(K key, CacheOptions options) {
      return null;
   }

   @Override
   public CloseableIterable<K> keys(CacheOptions options) {
      return null;
   }

   @Override
   public CloseableIterable<CacheEntry<K, V>> entries(CacheOptions options) {
      return null;
   }

   @Override
   public void putAll(Map<K, V> entries, CacheWriteOptions options) {

   }

   @Override
   public Map<K, V> getAll(Set<K> keys, CacheOptions options) {
      return null;
   }

   @Override
   public Map<K, V> getAll(CacheOptions options, K... keys) {
      return null;
   }

   @Override
   public Set<K> removeAll(Set<K> keys, CacheWriteOptions options) {
      return null;
   }

   @Override
   public long estimateSize(CacheOptions options) {
      return 0;
   }

   @Override
   public void clear(CacheOptions options) {
      cache.clear();
   }

   @Override
   public <R> SyncQuery<K, V, R> query(String query, CacheOptions options) {
      return new EmbeddedSyncQuery<>(cache.query(query), options);
   }

   @Override
   public AutoCloseable listen(SyncCacheEntryListener<K, V> listener) {
      return null;
   }

   @Override
   public <T> Set<CacheEntryProcessorResult<K, T>> process(Set<K> keys, SyncCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options) {
      return null;
   }

   @Override
   public <T> Set<CacheEntryProcessorResult<K, T>> processAll(SyncCacheEntryProcessor<K, V, T> processor, CacheProcessorOptions options) {
      return null;
   }

   @Override
   public SyncStreamingCache<K> streaming() {
      return new EmbeddedSyncStreamingCache<>(cache);
   }
}
