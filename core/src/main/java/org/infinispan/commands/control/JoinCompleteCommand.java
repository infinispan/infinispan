package org.infinispan.commands.control;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.remoting.transport.Address;

/**
 * // TODO: Manik: Document this
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = ReplicableCommandExternalizer.class, id = Ids.JOIN_COMPLETE_COMMAND)
public class JoinCompleteCommand extends BaseRpcCommand {
   public static final int COMMAND_ID = 21;
   Address joiner;
   DistributionManager distributionManager;

   public JoinCompleteCommand() {
   }

   public JoinCompleteCommand(String cacheName, Address joiner) {
      super(cacheName);
      this.joiner = joiner;
   }

   public void init(DistributionManager distributionManager) {
      this.distributionManager = distributionManager;
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      distributionManager.notifyJoinComplete(joiner);
      return null;
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
