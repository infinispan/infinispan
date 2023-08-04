package org.infinispan.hotrod.near;

import java.util.Iterator;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.hotrod.configuration.NearCache;
import org.infinispan.hotrod.configuration.NearCacheConfiguration;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Near cache based on {@link BoundedConcurrentMapNearCache}
 *
 * @since 14.0
 */
final class BoundedConcurrentMapNearCache<K, V> implements NearCache<K, V> {

   private final ConcurrentMap<K, CacheEntry<K, V>> map;
   private final Cache<K, CacheEntry<K, V>> cache;

   private BoundedConcurrentMapNearCache(Cache<K, CacheEntry<K, V>> cache) {
      this.cache = cache;
      this.map = cache.asMap();
   }

   public static <K, V> NearCache<K, V> create(final NearCacheConfiguration config,
                                               Consumer<? super CacheEntry<K, V>> removedConsumer) {
      Cache<K, CacheEntry<K, V>> cache = Caffeine.newBuilder()
            .maximumSize(config.maxEntries())
            .removalListener((key, value, cause) -> removedConsumer.accept(null))
            .build();
      return new BoundedConcurrentMapNearCache<>(cache);
   }

   @Override
   public boolean putIfAbsent(K key, CacheEntry<K, V> entry) {
      return map.putIfAbsent(key, entry) == null;
   }

   @Override
   public boolean replace(K key, CacheEntry<K, V> prevValue, CacheEntry<K, V> newValue) {
      return map.replace(key, prevValue, newValue);
   }

   @Override
   public boolean remove(K key) {
      return map.remove(key) != null;
   }

   @Override
   public boolean remove(K key, CacheEntry<K, V> entry) {
      return map.remove(key, entry);
   }

   @Override
   public CacheEntry<K, V> get(K key) {
      return map.get(key);
   }

   @Override
   public void clear() {
      map.clear();
   }

   @Override
   public int size() {
      // Make sure to clean up any evicted entries so the returned size is correct
      cache.cleanUp();
      return map.size();
   }

   @Override
   public Iterator<CacheEntry<K, V>> iterator() {
      return map.values().stream().iterator();
   }
}
