package org.infinispan;

/**
 * @deprecated Extend from
 * {@link org.infinispan.cache.impl.AbstractDelegatingCache}
 * instead. This class will be removed in the future.
 */
@Deprecated
public class AbstractDelegatingCache<K, V> extends org.infinispan.cache.impl.AbstractDelegatingCache<K, V> {
   public AbstractDelegatingCache(Cache<K, V> cache) {
      super(cache);
   }
}
