package org.infinispan.spring.embedded.provider;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.spring.common.provider.SpringCache;
import org.infinispan.util.concurrent.BlockingManager;

import jakarta.transaction.SystemException;
import jakarta.transaction.TransactionManager;

/**
 * A {@link SpringCache} backed by an embedded {@link Cache}.
 * <p>
 * When {@code putForExternalReadOnLoad} is enabled, values computed by a value loader on a cache miss
 * ({@link #get(Object, Callable)}, used by {@code @Cacheable(sync = true)}, and
 * {@link #retrieve(Object, Supplier)}) are stored with {@link Cache#putForExternalRead(Object, Object)}.
 * Such values come from the source of truth, so in invalidation mode they are stored locally without
 * invalidating the key on the other nodes. Explicit writes ({@link #put(Object, Object)}, used by
 * {@code @CachePut} and by {@code @Cacheable} without {@code sync}) and evictions still invalidate.
 * <p>
 * If a transaction is active on the cache's transaction manager, loaded values are stored with the default
 * {@code putIfAbsent} so that the write participates in the transaction and is discarded on rollback.
 *
 * @since 16.4
 */
public class SpringEmbeddedCache extends SpringCache {

   private final Cache<Object, Object> embeddedCache;
   private final boolean putForExternalReadOnLoad;

   /**
    * @param nativeCache              the embedded cache to delegate to
    * @param reactive                 whether reactive mode is enabled
    * @param putForExternalReadOnLoad whether values computed by a value loader on a cache miss are stored with
    *                                 {@link Cache#putForExternalRead(Object, Object)}
    */
   public SpringEmbeddedCache(Cache<Object, Object> nativeCache, boolean reactive, boolean putForExternalReadOnLoad) {
      super(nativeCache, reactive);
      this.embeddedCache = nativeCache;
      this.putForExternalReadOnLoad = putForExternalReadOnLoad;
   }

   /**
    * @return whether values computed by a value loader on a cache miss are stored with
    * {@link Cache#putForExternalRead(Object, Object)}
    */
   public boolean isPutForExternalReadOnLoad() {
      return putForExternalReadOnLoad;
   }

   @Override
   protected Object storeLoadedValue(Object key, Object encodedValue) {
      if (!usePutForExternalRead()) {
         return super.storeLoadedValue(key, encodedValue);
      }
      // putForExternalRead never overwrites an existing entry and fails silently;
      // the freshly loaded value is returned to the caller in any case.
      embeddedCache.putForExternalRead(key, encodedValue);
      return null;
   }

   @Override
   protected CompletableFuture<Object> storeLoadedValueAsync(Object key, Object encodedValue) {
      if (!usePutForExternalRead()) {
         return super.storeLoadedValueAsync(key, encodedValue);
      }
      // There is no asynchronous putForExternalRead, so run it on a blocking thread
      // to avoid blocking the caller's non-blocking thread.
      BlockingManager blockingManager = ComponentRegistry.componentOf(embeddedCache, BlockingManager.class);
      return blockingManager.runBlocking(() -> embeddedCache.putForExternalRead(key, encodedValue), key)
            .toCompletableFuture()
            .thenApply(ignore -> null);
   }

   private boolean usePutForExternalRead() {
      return putForExternalReadOnLoad && !isTransactionActive();
   }

   private boolean isTransactionActive() {
      TransactionManager tm = embeddedCache.getAdvancedCache().getTransactionManager();
      try {
         return tm != null && tm.getTransaction() != null;
      } catch (SystemException e) {
         throw new CacheException(e);
      }
   }
}
