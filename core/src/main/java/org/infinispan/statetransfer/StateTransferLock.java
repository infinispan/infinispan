package org.infinispan.statetransfer;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

import java.util.concurrent.TimeUnit;

/**
 * We use the state transfer lock for three different things:
 * <ol>
 *    <li>We don't want to execute a command until we have the transaction table for that topology id.
 *    For this purpose it works like a latch, commands wait on the latch and state transfer opens the latch
 *    when it has received all the transaction data for that topology id.</li>
 *    <li>Do not write anything to the data container in a segment that we have already removed.
 *    For this purpose, ownership checks and data container writes acquire a shared lock, and
 *    the segment removal acquires an exclusive lock.</li>
 *    <li>We want to handle state requests only after we have installed the same topology id, because
 *    this guarantees that we also have installed the corresponding view id and we have all the joiners
 *    in our JGroups view. Here it works like a latch as well, state requests wait on the latch and state
 *    transfer opens the latch when it has received all the transaction data for that topology id.</li>
 * </ol>
 *
 * @author anistor@redhat.com
 * @author Dan Berindei
 * @since 5.2
 */
@Scope(Scopes.NAMED_CACHE)
public interface StateTransferLock {

   // topology change lock
   void acquireExclusiveTopologyLock();

   void releaseExclusiveTopologyLock();

   void acquireSharedTopologyLock();

   void releaseSharedTopologyLock();

   // transaction data latch
   void notifyTransactionDataReceived(int topologyId);

   void waitForTransactionData(int expectedTopologyId, long timeout, TimeUnit unit) throws InterruptedException;

   boolean transactionDataReceived(int expectedTopologyId);

   // topology installation latch
   // TODO move this to Cluster/LocalTopologyManagerImpl and don't start requesting state until every node has the jgroups view with the local node
   void notifyTopologyInstalled(int topologyId);

   void waitForTopology(int expectedTopologyId, long timeout, TimeUnit unit) throws InterruptedException;

   boolean topologyReceived(int expectedTopologyId);
}
