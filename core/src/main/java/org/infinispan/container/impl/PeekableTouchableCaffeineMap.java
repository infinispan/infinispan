package org.infinispan.container.impl;

import java.util.concurrent.ConcurrentMap;

import org.infinispan.commons.util.AbstractDelegatingConcurrentMap;
import org.infinispan.container.entries.InternalCacheEntry;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Policy;

public class PeekableTouchableCaffeineMap<K, V> extends AbstractDelegatingConcurrentMap<K, InternalCacheEntry<K, V>>
      implements PeekableTouchableMap<K, V> {
   private final Cache<K, InternalCacheEntry<K, V>> caffeineCache;
   private final ConcurrentMap<K, InternalCacheEntry<K, V>> map;

   public PeekableTouchableCaffeineMap(Cache<K, InternalCacheEntry<K, V>> cache) {
      this.caffeineCache = cache;
      this.map = cache.asMap();
   }

   @Override
   protected ConcurrentMap<K, InternalCacheEntry<K, V>> delegate() {
      return map;
   }

   @Override
   public InternalCacheEntry<K, V> peek(Object key) {
      return caffeineCache.policy().getIfPresentQuietly((K) key);
   }

   @Override
   public void touchAll(long currentTimeMillis) {
      for (InternalCacheEntry<K, V> ice : map.values()) {
         ice.touch(currentTimeMillis);
      }
   }

   public void removeAll(Iterable<? extends K> iterable) {
      caffeineCache.invalidateAll(iterable);
   }

   public Policy<K, InternalCacheEntry<K, V>> policy() {
      return caffeineCache.policy();
   }

   public void cleanUp() {
      caffeineCache.cleanUp();
   }


}
