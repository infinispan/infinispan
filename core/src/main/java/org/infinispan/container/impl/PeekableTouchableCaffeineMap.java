package org.infinispan.container.impl;

import java.util.concurrent.ConcurrentMap;

import org.infinispan.commons.util.AbstractDelegatingConcurrentMap;
import org.infinispan.container.entries.InternalCacheEntry;

import com.github.benmanes.caffeine.cache.Cache;

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
   public boolean touchKey(Object key, long currentTimeMillis) {
      InternalCacheEntry<K, V> ice = peek(key);
      if (ice != null) {
         ice.touch(currentTimeMillis);
         return true;
      }
      return false;
   }

   @Override
   public void touchAll(long currentTimeMillis) {
      for (InternalCacheEntry<K, V> ice : map.values()) {
         ice.touch(currentTimeMillis);
      }
   }
}
