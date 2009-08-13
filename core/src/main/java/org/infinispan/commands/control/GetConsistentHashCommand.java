package org.infinispan.commands.control;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.remoting.transport.Address;

/**
 * Retrieves a consistent hash instance from the distribution manager.  This command is always sent to the coordinator
 * by a new joiner.
 * <p/>
 * // TODO rename to GET_ADDRESS_LIST_FROM_COORD, document accordingly
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = ReplicableCommandExternalizer.class, id = Ids.GET_CONSISTENT_HASH_COMMAND)
public class GetConsistentHashCommand extends BaseRpcCommand {

   public static final int COMMAND_ID = 17;
   DistributionManager distributionManager;
   Address joiner;

   public GetConsistentHashCommand() {
   }

   public GetConsistentHashCommand(String cacheName, Address joiner) {
      super(cacheName);
      this.joiner = joiner;
   }

   public GetConsistentHashCommand(DistributionManager distributionManager) {
      this.distributionManager = distributionManager;
   }

   public void initialize(DistributionManager distributionManager) {
      this.distributionManager = distributionManager;
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      return distributionManager.requestPermissionToJoin(joiner);
   }

   public byte getCommandId() {
      return COMMAND_ID;
   }

   public Object[] getParameters() {
      return new Object[]{cacheName, joiner};
   }

   public void setParameters(int commandId, Object[] parameters) {
      cacheName = (String) parameters[0];
      joiner = (Address) parameters[1];
   }
}
