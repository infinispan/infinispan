package org.infinispan.distribution;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.RehashControlCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.ch.ConsistentHashHelper;
import org.infinispan.distribution.ch.NodeTopologyInfo;
import org.infinispan.distribution.ch.TopologyInfo;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.infinispan.distribution.ch.ConsistentHashHelper.createConsistentHash;

/**
 * This task is kicked off whenever a MERGE is detected
 *
 * @author Manik Surtani
 * @since 4.2.1
 */
public class MergeTask extends JoinTask {


   public MergeTask(RpcManager rpcManager, CommandsFactory commandsFactory, Configuration conf,
            DataContainer dataContainer, DistributionManagerImpl dmi, InboundInvocationHandler inboundInvocationHandler,
            List<Address> newView, List<List<Address>> mergedGroups) {

      super(rpcManager, commandsFactory, conf, dataContainer, dmi, inboundInvocationHandler);

      TopologyInfo ti = buildTopologyInfo(newView);

      chNew = ConsistentHashHelper.createConsistentHash(configuration, newView, ti);

      if (mergedGroups.size() < 2) throw new IllegalArgumentException("Don't know how to handle a merge of " + mergedGroups.size() + " partitions!");
      if (mergedGroups.size() > 2) log.warn("Attempting to merge more than 2 partitions!  Inconsistencies may occur!  See https://issues.jboss.org/browse/ISPN-1001");

      List<Address> a1 = mergedGroups.get(0);
      List<Address> a2 = mergedGroups.get(1);

      TopologyInfo oldTopologyInfo = distributionManager.getTopologyInfo();
      if (!a1.contains(self)) chOld = createConsistentHash(configuration, a1, oldTopologyInfo);
      else if (!a2.contains(self)) chOld = createConsistentHash(configuration, a2, oldTopologyInfo);
      else throw new IllegalArgumentException("Neither of the merged partitions " +a1+ " and " + a2 + " contain " + self);
      distributionManager.setTopologyInfo(ti);
   }

   @SuppressWarnings("unchecked")
   private TopologyInfo buildTopologyInfo(List<Address> newView) {
      if (configuration.getGlobalConfiguration().hasTopologyInfo()) {
         TopologyInfo oldTI = distributionManager.getTopologyInfo();
         // we are using topologies
         Set<Address> unknownAddresses = new HashSet<Address>();
         for (Address a: newView) {
            if (!oldTI.containsInfoForNode(a)) unknownAddresses.add(a);
         }

         if (!unknownAddresses.isEmpty()) {
            Collection<NodeTopologyInfo> moreTopologies = null;
            for (Address topologyProvider : unknownAddresses) {
               Map<Address, Response> r = rpcManager.invokeRemotely(Collections.singleton(topologyProvider), cf.buildRehashControlCommand(RehashControlCommand.Type.FETCH_TOPOLOGY_INFO, self), true, true);
               Response resp = r.get(topologyProvider);
               if (resp.isSuccessful() && resp.isValid()) {
                  // we have the response we need!
                  moreTopologies = (Collection<NodeTopologyInfo>) ((SuccessfulResponse) resp).getResponseValue();
                  break;
               }
            }

            if (moreTopologies == null) throw new CacheException("Unable to retrieve topology information for addresses " + unknownAddresses);
            return new TopologyInfo(oldTI, moreTopologies);
         }
         return oldTI;
      } else {
         // no topologies are used in this config
         return null;
      }
   }

   @Override
   protected void getPermissionToJoin() {
      // don't need to "ask" for permission in this case
   }

   @Override
   protected void broadcastNewConsistentHash() {
      // this is no longer necessary; a no-op
   }

   @Override
   protected void signalJoinRehashEnd() {
      // this is no longer necessary; a no-op
   }
}
