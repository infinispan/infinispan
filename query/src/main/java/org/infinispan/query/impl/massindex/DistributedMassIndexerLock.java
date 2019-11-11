package org.infinispan.query.impl.massindex;

import org.infinispan.Cache;
import org.infinispan.lock.EmbeddedClusteredLockManagerFactory;
import org.infinispan.lock.api.ClusteredLock;
import org.infinispan.lock.api.ClusteredLockManager;
import org.infinispan.query.logging.Log;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.LogFactory;

/**
 * A cluster-wide lock to prevent multiple instances of the {@link org.infinispan.query.MassIndexer} to run concurrently.
 *
 * @since 10.1
 */
final class DistributedMassIndexerLock implements MassIndexLock {
   private static final Log LOG = LogFactory.getLog(DistributedMassIndexerLock.class, Log.class);
   private final String lockName;
   private volatile ClusteredLock clusteredLock;
   private final Cache<?, ?> cache;

   DistributedMassIndexerLock(Cache<?, ?> cache) {
      this.cache = cache;
      this.lockName = String.format("massIndexer-%s", cache.getName());
   }

   @Override
   public boolean lock() {
      try {
         return getLock().tryLock().get();
      } catch (Exception e) {
         throw LOG.errorAcquiringMassIndexerLock(e);
      }
   }

   @Override
   public void unlock() {
      try {
         getLock().unlock();
      } catch (Exception e) {
         throw LOG.errorReleasingMassIndexerLock(e);
      }
   }

   @Override
   public boolean isAcquired() {
      return CompletionStages.join(getLock().isLocked());
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
