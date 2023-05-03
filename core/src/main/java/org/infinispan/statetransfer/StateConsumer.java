package org.infinispan.statetransfer;

import java.util.Collection;
import java.util.concurrent.CompletionStage;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;

/**
 * Handles inbound state transfers.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Scope(Scopes.NAMED_CACHE)
public interface StateConsumer {

   boolean isStateTransferInProgress();

   boolean isStateTransferInProgressForKey(Object key);

   /**
    * Returns the number of in-flight requested segments.
    */
   long inflightRequestCount();

   /**
    * Returns the number of in-flight transactional requested segments.
    */
   long inflightTransactionSegmentCount();

   /**
    * Receive notification of topology changes. {@link org.infinispan.commands.statetransfer.StateTransferStartCommand},
    * are issued for segments that are new to this
    * member and the segments that are no longer owned are discarded.
    *
    * @return completion stage that is completed when the topology update is processed,
    * wrapping another completion stage that is completed when the state transfer has finished
    */
   CompletionStage<CompletionStage<Void>> onTopologyUpdate(CacheTopology cacheTopology, boolean isRebalance);

   CompletionStage<?> applyState(Address sender, int topologyId, Collection<StateChunk> stateChunks);

   /**
    * Cancels all incoming state transfers. The already received data is not discarded.
    * This is executed when the cache is shutting down.
    */
   void stop();

   /**
    * Stops applying incoming state. Also stops tracking updated keys. Should be called at the end of state transfer or
    * when a ClearCommand is committed during state transfer.
    *
    * @param topologyId Topology id at the end of state transfer
    */
   void stopApplyingState(int topologyId);

   /**
    * @return  true if this node has already received the first rebalance command
    */
   boolean ownsData();
}
