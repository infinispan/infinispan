package org.infinispan.container.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.commons.util.AbstractDelegatingConcurrentMap;
import org.infinispan.container.entries.InternalCacheEntry;

public class PeekableTouchableContainerMap<K, V> extends AbstractDelegatingConcurrentMap<K, InternalCacheEntry<K, V>>
      implements PeekableTouchableMap<K, V> {
   private final ConcurrentMap<K, InternalCacheEntry<K, V>> map;

   public PeekableTouchableContainerMap() {
      this(new ConcurrentHashMap<>());
   }

   public PeekableTouchableContainerMap(ConcurrentMap<K, InternalCacheEntry<K, V>> map) {
      this.map = map;
   }

   @Override
   protected ConcurrentMap<K, InternalCacheEntry<K, V>> delegate() {
      return map;
   }

   @Override
   public InternalCacheEntry<K, V> peek(Object key) {
      return delegate().get(key);
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
}
