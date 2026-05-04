package org.infinispan.hibernate.cache.commons.access;

import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.expiration.impl.ExpirationManagerImpl;

/**
 * Expiration manager that touches entries synchronously to avoid async RPC.
 * <p>
 * {@link ExpirationManagerImpl#checkExpiredMaxIdle} calls {@code cache.touch()} which goes through
 * the interceptor chain including distribution interceptors, causing async RPC in clustered mode.
 * This makes the interceptor chain async and breaks the assumption in
 * {@link TombstoneAccessDelegate#putFromLoad} that the functional map eval completes synchronously.
 * <p>
 * This implementation touches the entry directly in memory instead, which is sufficient since
 * the Hibernate 2LC already replaces the clustered expiration manager with a local-only one.
 */
public class SyncLocalExpirationManager<K, V> extends ExpirationManagerImpl<K, V> {
   @Override
   protected CompletionStage<Boolean> checkExpiredMaxIdle(InternalCacheEntry<?, ?> entry, int segment, long currentTime) {
      entry.touch(currentTime);
      return CompletableFutures.completedFalse();
   }
}
