package org.infinispan.statetransfer;

import java.util.Map;
import java.util.Set;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.jmx.annotations.DataType;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;

/**
 * A component that manages the state transfer when the topology of the cluster changes.
 *
 * @author Dan Berindei <dan@infinispan.org>
 * @author Mircea Markus
 * @author anistor@redhat.com
 * @since 5.1
 */
@Scope(Scopes.NAMED_CACHE)
@MBean(objectName = "StateTransferManager", description = "Component that handles state transfer")
public interface StateTransferManager {

   //todo [anistor] this is inaccurate. this node does not hold state yet in current implementation
   @ManagedAttribute(description = "If true, the node has successfully joined the grid and is considered to hold state.  If false, the join process is still in progress.", displayName = "Is join completed?", dataType = DataType.TRAIT)
   boolean isJoinComplete();

   /**
    * Checks if an inbound state transfer is in progress.
    */
   @ManagedAttribute(description = "Checks whether there is a pending inbound state transfer on this cluster member.", displayName = "Is state transfer in progress?", dataType = DataType.TRAIT)
   boolean isStateTransferInProgress();

   /**
    * Checks if an inbound state transfer is in progress for a given key.
    *
    * @param key
    * @return
    */
   boolean isStateTransferInProgressForKey(Object key);

   CacheTopology getCacheTopology();

   void start() throws Exception;

   void stop();

   /**
    * If there is an state transfer happening at the moment, this method forwards the supplied
    * command to the nodes that are new owners of the data, in order to assure consistency.
    */
   Map<Address, Response> forwardCommandIfNeeded(TopologyAffectedCommand command, Set<Object> affectedKeys, Address origin);

   void notifyEndOfStateTransfer(int topologyId, int rebalanceId);

   /**
    * @return  true if this node has already received the first rebalance start
    */
   boolean ownsData();

   /**
    * @return The id of the first cache topology in which the local node was a member
    *    (even if it didn't own any data).
    */
   int getFirstTopologyAsMember();

   @ManagedAttribute(description = "Retrieves the rebalancing status for this cache. Possible values are PENDING, SUSPENDED, IN_PROGRESS, BALANCED", displayName = "Rebalancing progress", dataType = DataType.TRAIT)
   String getRebalancingStatus() throws Exception;
}
