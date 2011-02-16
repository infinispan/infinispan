package org.infinispan.interceptors;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;

/**
 * A subclass of the locking interceptor that is able to differentiate committing changes on a ReadCommittedEntry for
 * storage in the main cache or in L1, used by DIST
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DistLockingInterceptor extends LockingInterceptor {
   DistributionManager dm;

   @Inject
   public void injectDistributionManager(DistributionManager dm) {
      this.dm = dm;
   }

   @Override
   protected void commitEntry(CacheEntry entry) {
      boolean doCommit = true;
      if (!dm.getLocality(entry.getKey()).isLocal()) {
         if (configuration.isL1CacheEnabled()) {
            dm.transformForL1(entry);
         } else {
            doCommit = false;
         }
      }
      if (doCommit)
         entry.commit(dataContainer);
      else
         entry.rollback();
   }
}
