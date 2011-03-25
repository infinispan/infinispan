package org.infinispan.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.ch.ConsistentHashHelper;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;

import java.util.List;

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
     // TODO how do we get TopologyInfo off nodes during a merge view?
      chNew = ConsistentHashHelper.createConsistentHash(configuration, newView, null);

      if (mergedGroups.size() < 2) throw new IllegalArgumentException("Don't know how to handle a merge of " + mergedGroups.size() + " partitions!");
      if (mergedGroups.size() > 2) log.warn("Attempting to merge more than 2 partitions!  Inconsistencies may occur!  See https://issues.jboss.org/browse/ISPN-1001");

      List<Address> a1 = mergedGroups.get(0);
      List<Address> a2 = mergedGroups.get(1);

      // TODO how do we get TopologyInfo off nodes during a merge view?
      if (!a1.contains(self)) chOld = createConsistentHash(configuration, a1, null);
      else if (!a2.contains(self)) chOld = createConsistentHash(configuration, a2, null);
      else throw new IllegalArgumentException("Neither of the merged partitions " +a1+ " and " + a2 + " contain " + self);
   }

   @Override
   protected boolean getPermissionToJoin() {
      // don't need to "ask" for permission in this case
      return false;
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
