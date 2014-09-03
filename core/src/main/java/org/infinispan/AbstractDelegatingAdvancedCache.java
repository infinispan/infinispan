package org.infinispan;

/**
 * @deprecated Extend from
 * {@link org.infinispan.cache.impl.AbstractDelegatingAdvancedCache}
 * instead. This class will be removed in the future.
 */
@Deprecated
public class AbstractDelegatingAdvancedCache<K, V> extends org.infinispan.cache.impl.AbstractDelegatingAdvancedCache<K, V> {
   public AbstractDelegatingAdvancedCache(AdvancedCache<K, V> cache) {
      super(cache);
   }

   public AbstractDelegatingAdvancedCache(AdvancedCache<K, V> cache, AdvancedCacheWrapper<K, V> wrapper) {
      super(cache, wrapper);
   }
}
