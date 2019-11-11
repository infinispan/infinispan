package org.infinispan.query.impl.massindex;

import org.infinispan.Cache;

/**
 * @since 10.1
 */
class MassIndexerLockFactory {
   private MassIndexerLockFactory() {
   }

   static MassIndexLock buildLock(Cache<?, ?> cache) {
      if (cache.getCacheConfiguration().clustering().cacheMode().isClustered())
         return new DistributedMassIndexerLock(cache);
      return new LocalMassIndexerLock();
   }
}
