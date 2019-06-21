package org.infinispan.statetransfer;

import java.util.Map;
import java.util.Set;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;

/**
 * A component that manages the state transfer when the topology of the cluster changes.
 *
 * @author Dan Berindei &lt;dan@infinispan.org&gt;
 * @author Mircea Markus
 * @author anistor@redhat.com
 * @since 5.1
 */
@Scope(Scopes.NAMED_CACHE)
public interface StateTransferManager {

   //todo [anistor] this is inaccurate. this node does not hold state yet in current implementation
   boolean isJoinComplete();

   /**
    * Checks if an inbound state transfer is in progress.
    */
   boolean isStateTransferInProgress();

   /**
    * Checks if an inbound state transfer is in progress for a given key.
    *
    * @param key
    * @return
    */
   boolean isStateTransferInProgressForKey(Object key);

   /**
    * @deprecated Since 9.3, please use {@link DistributionManager#getCacheTopology()} instead.
    */
   @Deprecated
   CacheTopology getCacheTopology();

   void start() throws Exception;

   void waitForInitialStateTransferToComplete();

   void stop();

   /**
    * If there is an state transfer happening at the moment, this method forwards the supplied
    * command to the nodes that are new owners of the data, in order to assure consistency.
    */
   Map<Address, Response> forwardCommandIfNeeded(TopologyAffectedCommand command, Set<Object> affectedKeys, Address origin);

   /**
    * @return  true if this node has already received the first rebalance start
    * @deprecated Since 9.4, will be removed.
    */
   @Deprecated
   boolean ownsData();

   /**
    * @return The id of the first cache topology in which the local node was a member
    *    (even if it didn't own any data).
    *
    * @deprecated Since 9.4, will be removed.
    */
   @Deprecated
   int getFirstTopologyAsMember();

   String getRebalancingStatus() throws Exception;

   StateConsumer getStateConsumer();

   StateProvider getStateProvider();
}
