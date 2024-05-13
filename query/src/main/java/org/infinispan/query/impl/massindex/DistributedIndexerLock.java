package org.infinispan.query.impl.massindex;

import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.globalstate.impl.ConfigCacheLock;

/**
 * A cluster-wide lock to prevent multiple instances of the {@link org.infinispan.query.Indexer} to run concurrently.
 *
 * @since 10.1
 */
final class DistributedIndexerLock implements IndexLock {
   private final ConfigCacheLock lock;

   DistributedIndexerLock(Cache<?, ?> cache) {
      this.lock = new ConfigCacheLock(String.format("massIndexer-%s", cache.getName()), cache.getCacheManager());
   }

   @Override
   public CompletionStage<Boolean> lock() {
      return lock.tryLock();
   }

   @Override
   public CompletionStage<Void> unlock() {
      return lock.unlock();
   }
}
