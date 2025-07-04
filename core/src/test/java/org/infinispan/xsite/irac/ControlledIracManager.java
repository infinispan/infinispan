package org.infinispan.xsite.irac;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.stream.Stream;

import org.infinispan.commands.RequestUUID;
import org.infinispan.commons.util.IntSet;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.xsite.statetransfer.XSiteState;

/**
 * An {@link IracManager} implementation that can be controlled for testing purpose.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Scope(Scopes.NAMED_CACHE)
public class ControlledIracManager implements IracManager {

   protected final IracManager actual;

   public ControlledIracManager(IracManager actual) {
      this.actual = actual;
   }

   @Override
   public void trackUpdatedKey(int segment, Object key, RequestUUID owner) {
      actual.trackUpdatedKey(segment, key, owner);
   }

   @Override
   public void trackExpiredKey(int segment, Object key, RequestUUID owner) {
      actual.trackExpiredKey(segment, key, owner);
   }

   @Override
   public CompletionStage<Void> trackForStateTransfer(Collection<XSiteState> stateList) {
      return actual.trackForStateTransfer(stateList);
   }

   @Override
   public void trackClear(boolean sendClear) {
      actual.trackClear(sendClear);
   }

   @Override
   public void removeState(IracManagerKeyInfo state) {
      actual.removeState(state);
   }

   @Override
   public void onTopologyUpdate(CacheTopology oldCacheTopology, CacheTopology newCacheTopology) {
      actual.onTopologyUpdate(oldCacheTopology, newCacheTopology);
   }

   @Override
   public void requestState(Address requestor, IntSet segments) {
      actual.requestState(requestor, segments);
   }

   @Override
   public void receiveState(int segment, Object key, RequestUUID owner, IracMetadata tombstone) {
      actual.receiveState(segment, key, owner, tombstone);
   }

   @Override
   public CompletionStage<Boolean> checkAndTrackExpiration(Object key) {
      return actual.checkAndTrackExpiration(key);
   }

   @Override
   public void incrementNumberOfDiscards() {
      actual.incrementNumberOfDiscards();
   }

   @Override
   public void incrementNumberOfConflictLocalWins() {
      actual.incrementNumberOfConflictLocalWins();
   }

   @Override
   public void incrementNumberOfConflictRemoteWins() {
      actual.incrementNumberOfConflictRemoteWins();
   }

   @Override
   public void incrementNumberOfConflictMerged() {
      actual.incrementNumberOfConflictMerged();
   }

   @Override
   public boolean containsKey(Object key) {
      return actual.containsKey(key);
   }

   @Override
   public Stream<IracManagerKeyInfo> pendingKeys() {
      return actual.pendingKeys();
   }

   @Override
   public void checkStaleKeys(Address origin, Collection<IracManagerKeyInfo> keys) {
      actual.checkStaleKeys(origin, keys);
   }

   protected Optional<DefaultIracManager> asDefaultIracManager() {
      return actual instanceof DefaultIracManager ? Optional.of((DefaultIracManager) actual) : Optional.empty();
   }
}
