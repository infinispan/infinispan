package org.infinispan.cache.impl;

import java.util.Map;
import java.util.function.Function;

import org.infinispan.commons.util.EnumUtil;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ForwardingCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.stream.StreamMarshalling;

/**
 * Entry set backed by a cache.
 *
 * <p>Implements {@code CacheSet<CacheEntry<K, V>>} but it is also (mis-)used as a {@code CacheSet<Map.Entry<K, V>>}.
 * This works because {@code add()} and {@code addAll()} are not implemented.</p>
 *
 * @since 13.0
 */
public class CacheBackedEntrySet<K, V> extends AbstractCacheBackedSet<K, V, CacheEntry<K, V>> {
   public CacheBackedEntrySet(CacheImpl<K, V> cache, Object lockOwner, long explicitFlags) {
      super(cache, lockOwner, explicitFlags);
   }

   @Override
   public boolean contains(Object o) {
      if (!(o instanceof Map.Entry))
         return false;

      Map.Entry<?, ?> entry = (Map.Entry<?, ?>) o;

      InvocationContext ctx = cache.invocationContextFactory.createInvocationContext(false, 1);
      if (lockOwner != null) {
         ctx.setLockOwner(lockOwner);
      }
      V cacheValue = cache.get(entry.getKey(), explicitFlags, ctx);

      return cacheValue != null && cacheValue.equals(entry.getValue());
   }

   @Override
   protected Function<Map.Entry<K, V>, ?> entryToKeyFunction() {
      return StreamMarshalling.entryToKeyFunction();
   }

   @Override
   protected Object extractKey(Object e) {
      if (!(e instanceof Map.Entry))
         return null;

      return ((Map.Entry<?, ?>) e).getKey();
   }

   @Override
   protected CacheEntry<K, V> wrapElement(CacheEntry<K, V> e) {
      return new ForwardingCacheEntry<K, V>() {
         @Override
         protected CacheEntry<K, V> delegate() {
            return e;
         }

         @Override
         public V setValue(V value) {
            cache.put(getKey(), value, cache.defaultMetadata, EnumUtil.EMPTY_BIT_SET,
                      decoratedWriteContextBuilder());
            return super.setValue(value);
         }
      };
   }
}
