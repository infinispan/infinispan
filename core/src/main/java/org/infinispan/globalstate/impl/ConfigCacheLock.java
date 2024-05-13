package org.infinispan.globalstate.impl;

import static org.infinispan.commons.internal.InternalCacheNames.CONFIG_STATE_CACHE_NAME;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.manager.EmbeddedCacheManager;

/**
 * A basic locking mechanism that uses the {@link org.infinispan.commons.internal.InternalCacheNames#CONFIG_STATE_CACHE_NAME}
 * to ensure only one operation can proceed.
 */
public class ConfigCacheLock {
   final ScopedState state;
   final Cache<ScopedState, Boolean> lockCache;

   public ConfigCacheLock(String name, EmbeddedCacheManager cm) {
      this.state = new ScopedState("___internal_lock", name);
      this.lockCache = cm.getCache(CONFIG_STATE_CACHE_NAME);
      this.lockCache.putIfAbsent(state, false);
   }

   /**
    * Acquires the lock only if it is free at the time of invocation.
    * Acquires the lock if it is available and returns a {@link CompletableFuture} with the value {@code true}.
    * If the lock is not available a {@link CompletableFuture} with the value {@code false} is returned.
    */
   public CompletionStage<Boolean> tryLock() {
      return lockCache.replaceAsync(state, false, true);
   }

   /**
    * Releases the lock if it is currently held.
    */
   public CompletionStage<Void> unlock() {
      return lockCache.replaceAsync(state, true, false)
            .thenApply(v -> null);
   }

   @Override
   public String toString() {
      return "CompareAndSetLock{" +
            "state=" + state +
            '}';
   }
}
