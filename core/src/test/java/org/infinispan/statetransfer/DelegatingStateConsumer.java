package org.infinispan.statetransfer;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;

public class DelegatingStateConsumer implements StateConsumer {

   private final StateConsumer delegate;

   public DelegatingStateConsumer(StateConsumer delegate) {
      this.delegate = delegate;
   }

   @Override
   public boolean isStateTransferInProgress() {
      return delegate.isStateTransferInProgress();
   }

   @Override
   public boolean isStateTransferInProgressForKey(Object key) {
      return delegate.isStateTransferInProgressForKey(key);
   }

   @Override
   public CompletableFuture<Void> onTopologyUpdate(CacheTopology cacheTopology, boolean isRebalance) {
      return delegate.onTopologyUpdate(cacheTopology, isRebalance);
   }

   @Override
   public CompletionStage<?> applyState(Address sender, int topologyId, boolean pushTransfer, Collection<StateChunk> stateChunks) {
      return delegate.applyState(sender, topologyId, pushTransfer, stateChunks);
   }

   @Override
   public void stop() {
      delegate.stop();
   }

   @Override
   public void stopApplyingState(int topologyId) {
      delegate.stopApplyingState(topologyId);
   }

   @Override
   public boolean ownsData() {
      return delegate.ownsData();
   }
}
