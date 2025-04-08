package org.infinispan.hibernate.cache.v62.impl;

import java.util.concurrent.CompletableFuture;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.infinispan.hibernate.cache.commons.InfinispanDataRegion;
import org.infinispan.hibernate.cache.commons.util.Tombstone;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class TombstoneAccessDelegate extends org.infinispan.hibernate.cache.commons.access.TombstoneAccessDelegate {
   public TombstoneAccessDelegate(InfinispanDataRegion region) {
      super(region);
   }

   @Override
   protected void write(Object session, Object key, Object value) {
      Sync sync = (Sync) ((SharedSessionContractImplementor) session).getCacheTransactionSynchronization();
      FutureUpdateInvocation invocation = new FutureUpdateInvocation(asyncWriteMap, key, value, region, sync.getCachingTimestamp());
      sync.registerAfterCommit(invocation);
      // The update will be invalidating all putFromLoads for the duration of expiration or until removed by the synchronization
      Tombstone tombstone = new Tombstone(invocation.getUuid(), region.nextTimestamp() + region.getTombstoneExpiration());
      CompletableFuture<Void> future = writeMap.eval(key, tombstone);
      if (tombstone.isComplete()) {
         sync.registerBeforeCommit(future);
      } else {
         log.trace("Tombstone was not applied immediately, waiting.");
         // Slow path: there's probably a locking conflict and we need to wait
         // until the command gets applied (and replicated, too!).
         future.join();
      }
   }
}
