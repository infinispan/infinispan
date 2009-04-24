package org.infinispan.interceptors;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
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
   protected void commitEntry(InvocationContext ctx, CacheEntry entry) {
      if (!dm.isLocal(entry.getKey())) dm.transformForL1(entry);
      entry.commit(dataContainer);
   }
}
