package org.infinispan.xsite.irac;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.IntSet;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.xsite.statetransfer.XSiteState;

/**
 * A no-op implementation of {@link IracManager} for cache without asynchronous remote site backups.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Scope(Scopes.NAMED_CACHE)
public enum NoOpIracManager implements IracManager {
   INSTANCE;

   @Override
   public void trackUpdatedKey(int segment, Object key, Object lockOwner) {
      // no-op
   }

   @Override
   public void trackExpiredKey(int segment, Object key, Object lockOwner) {
      // no-op
   }

   @Override
   public CompletionStage<Void> trackForStateTransfer(Collection<XSiteState> stateList) {
      return CompletableFutures.completedNull();
   }

   @Override
   public void trackClear(boolean sendClear) {
      // no-op
   }

   @Override
   public void removeState(IracManagerKeyInfo state) {
      //no-op
   }

   @Override
   public void onTopologyUpdate(CacheTopology oldCacheTopology, CacheTopology newCacheTopology) {
      // no-op
   }

   @Override
   public void requestState(Address requestor, IntSet segments) {
      // no-op
   }

   @Override
   public void receiveState(int segment, Object key, Object lockOwner, IracMetadata tombstone) {
      // no-op
   }

   @Override
   public CompletionStage<Boolean> checkAndTrackExpiration(Object key) {
      return CompletableFutures.completedTrue();
   }

   @Override
   public void incrementNumberOfDiscards() {
      // no-op
   }

   @Override
   public void incrementNumberOfConflictLocalWins() {
      // no-op
   }

   @Override
   public void incrementNumberOfConflictRemoteWins() {
      // no-op
   }

   @Override
   public void incrementNumberOfConflictMerged() {
      // no-op
   }

   @Override
   public boolean containsKey(Object key) {
      return false;
   }
}
