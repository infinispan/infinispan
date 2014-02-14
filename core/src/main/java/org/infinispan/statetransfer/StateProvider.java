package org.infinispan.statetransfer;

import org.infinispan.distexec.DistributedCallable;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;

import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Handles outbound state transfers.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Scope(Scopes.NAMED_CACHE)
public interface StateProvider {

   boolean isStateTransferInProgress();

   /**
    * Receive notification of topology changes. Cancels all outbound transfers to destinations that are no longer members.
    * The other outbound transfers remain unaffected.
    *
    * @param cacheTopology
    * @param isRebalance
    */
   void onTopologyUpdate(CacheTopology cacheTopology, boolean isRebalance);

   /**
    * Gets the list of transactions that affect keys from the given segments. This is invoked in response to a
    * StateRequestCommand of type StateRequestCommand.Type.GET_TRANSACTIONS.
    *
    * @param destination the address of the requester
    * @param topologyId
    * @param segments
    * @return list transactions and locks for the given segments
    */
   List<TransactionInfo> getTransactionsForSegments(Address destination, int topologyId, Set<Integer> segments) throws InterruptedException;

   Collection<DistributedCallable> getClusterListenersToInstall();

   /**
    * Start to send cache entries that belong to the given set of segments. This is invoked in response to a
    * StateRequestCommand of type StateRequestCommand.Type.START_STATE_TRANSFER.
    *
    * @param destination the address of the requester
    * @param topologyId
    * @param segments
    */
   void startOutboundTransfer(Address destination, int topologyId, Set<Integer> segments) throws InterruptedException;

   /**
    * Cancel sending of cache entries that belong to the given set of segments. This is invoked in response to a
    * StateRequestCommand of type StateRequestCommand.Type.CANCEL_STATE_TRANSFER.
    *
    * @param destination the address of the requester
    * @param topologyId
    * @param segments    the segments that we have to cancel transfer for
    */
   void cancelOutboundTransfer(Address destination, int topologyId, Set<Integer> segments);

   void start();

   /**
    * Cancels all outbound state transfers.
    * This is executed when the cache is shutting down.
    */
   void stop();
}
