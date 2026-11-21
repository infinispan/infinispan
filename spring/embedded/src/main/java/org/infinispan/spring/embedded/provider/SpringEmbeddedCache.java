package org.infinispan.spring.embedded.provider;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.infinispan.Cache;
import org.infinispan.spring.common.provider.SpringCache;

/**
 * A {@link SpringCache} backed by an embedded {@link Cache}.
 * <p>
 * When {@code putForExternalReadOnLoad} is enabled, values computed by a value loader on a cache miss
 * ({@link #get(Object, Callable)}, used by {@code @Cacheable(sync = true)}, and
 * {@link #retrieve(Object, Supplier)}) are stored with {@link Cache#putForExternalRead(Object, Object)}.
 * Such values come from the source of truth, so in invalidation mode they are stored locally without
 * invalidating the key on the other nodes. Explicit writes ({@link #put(Object, Object)}, used by
 * {@code @CachePut} and by {@code @Cacheable} without {@code sync}) and evictions still invalidate.
 *
 * @since 16.4
 */
public class SpringEmbeddedCache extends SpringCache {

   private final Cache<Object, Object> embeddedCache;
   private final boolean putForExternalReadOnLoad;

   public SpringEmbeddedCache(Cache<Object, Object> nativeCache, boolean reactive, boolean putForExternalReadOnLoad) {
      super(nativeCache, reactive);
      this.embeddedCache = nativeCache;
      this.putForExternalReadOnLoad = putForExternalReadOnLoad;
   }

   public boolean isPutForExternalReadOnLoad() {
      return putForExternalReadOnLoad;
   }

   @Override
   protected Object storeLoadedValue(Object key, Object encodedValue) {
      if (!putForExternalReadOnLoad) {
         return super.storeLoadedValue(key, encodedValue);
      }
      // putForExternalRead never overwrites an existing entry and fails silently;
      // the freshly loaded value is returned to the caller in any case.
      embeddedCache.putForExternalRead(key, encodedValue);
      return null;
   }

   @Override
   protected CompletableFuture<Object> storeLoadedValueAsync(Object key, Object encodedValue) {
      if (!putForExternalReadOnLoad) {
         return super.storeLoadedValueAsync(key, encodedValue);
      }
      try {
         embeddedCache.putForExternalRead(key, encodedValue);
         return CompletableFuture.completedFuture(null);
      } catch (Throwable t) {
         return CompletableFuture.failedFuture(t);
      }
   }
}
