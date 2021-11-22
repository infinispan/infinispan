package org.infinispan.cache.impl;

import java.util.Map;
import java.util.function.Function;

import org.infinispan.context.InvocationContext;

/**
 * Key set backed by a cache.
 *
 * @since 13.0
 */
public class CacheBackedKeySet<K, V> extends AbstractCacheBackedSet<K, V, K> {
   public CacheBackedKeySet(CacheImpl<K, V> cache, Object lockOwner, long explicitFlags) {
      super(cache, lockOwner, explicitFlags);
   }

    @Override
    public boolean contains(Object o) {
       InvocationContext ctx = cache.invocationContextFactory.createInvocationContext(false, 1);
       return cache.containsKey(o, explicitFlags, ctx);
    }

    @Override
   protected Function<Map.Entry<K, V>, ?> entryToKeyFunction() {
      return null;
   }

   @Override
   protected Object extractKey(Object e) {
      return e;
   }

   @Override
   protected K wrapElement(K e) {
         return e;
   }
}
