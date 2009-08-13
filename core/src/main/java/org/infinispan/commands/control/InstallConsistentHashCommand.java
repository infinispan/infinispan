package org.infinispan.commands.control;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.remoting.transport.Address;

/**
 * Installs a consistent hash in a distribution manager
 * <p/>
 * // TODO rename to INFORM_REHASH_ON_JOIN
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = ReplicableCommandExternalizer.class, id = Ids.INSTALL_CONSISTENT_HASH_COMMAND)
public class InstallConsistentHashCommand extends BaseRpcCommand {

   public static final int COMMAND_ID = 18;
   DistributionManager distributionManager;
   Address joiner;
   boolean starting; // if true the rehash is starting; if false it has completed.

   public InstallConsistentHashCommand() {
   }

   public InstallConsistentHashCommand(String cacheName, Address joiner, boolean starting) {
      super(cacheName);
      this.joiner = joiner;
      this.starting = starting;
   }

   public void initialize(DistributionManager distributionManager) {
      this.distributionManager = distributionManager;
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      distributionManager.informRehashOnJoin(joiner, starting);
      return null;
   }

   public byte getCommandId() {
      return COMMAND_ID;
   }

   public Object[] getParameters() {
      return new Object[]{cacheName, joiner, starting};
   }

   public void setParameters(int commandId, Object[] parameters) {
      cacheName = (String) parameters[0];
      joiner = (Address) parameters[1];
      starting = (Boolean) parameters[2];
   }
}
