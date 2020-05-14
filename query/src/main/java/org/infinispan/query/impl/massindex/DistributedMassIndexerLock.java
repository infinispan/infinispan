package org.infinispan.query.impl.massindex;

import java.util.concurrent.CompletionStage;

import org.infinispan.Cache;
import org.infinispan.lock.EmbeddedClusteredLockManagerFactory;
import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockManager;

/**
 * A cluster-wide lock to prevent multiple instances of the {@link org.infinispan.query.MassIndexer} to run concurrently.
 *
 * @since 10.1
 */
final class DistributedMassIndexerLock implements MassIndexLock {
   private final String lockName;
   private volatile ClusteredLock clusteredLock;
   private final Cache<?, ?> cache;

   DistributedMassIndexerLock(Cache<?, ?> cache) {
      this.cache = cache;
      this.lockName = String.format("massIndexer-%s", cache.getName());
   }

   @Override
   public CompletionStage<Boolean> lock() {
      return getLock().tryLock();
   }

   @Override
   public CompletionStage<Void> unlock() {
      return getLock().unlock();
   }

   private ClusteredLock getLock() {
      if (clusteredLock == null) {
         synchronized (this) {
            if (clusteredLock == null) {
               ClusteredLockManager clusteredLockManager = EmbeddedClusteredLockManagerFactory.from(cache.getCacheManager());
               boolean isDefined = clusteredLockManager.isDefined(lockName);
               if (!isDefined) {
                  clusteredLockManager.defineLock(lockName);
               }
               clusteredLock = clusteredLockManager.get(lockName);
            }
         }
      }
      return clusteredLock;
   }
}
