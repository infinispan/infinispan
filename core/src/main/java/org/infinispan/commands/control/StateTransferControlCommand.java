package org.infinispan.commands.control;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.marshall.Ids;
import org.infinispan.marshall.Marshallable;
import org.infinispan.marshall.exts.ReplicableCommandExternalizer;
import org.infinispan.remoting.transport.Transport;

/**
 * A command that informs caches participating in a state transfer of the various stages in the state transfer process.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Marshallable(externalizer = ReplicableCommandExternalizer.class, id = Ids.STATE_TRANSFER_CONTROL_COMMAND)
public class StateTransferControlCommand implements ReplicableCommand {
   public static final int COMMAND_ID = 15;
   Transport transport;
   boolean enabled;

   public StateTransferControlCommand() {
   }

   public StateTransferControlCommand(boolean enabled) {
      this.enabled = enabled;
   }

   public void init(Transport transport) {
      this.transport = transport;
   }

   public Object perform(InvocationContext ctx) throws Throwable {
      if (enabled)
         transport.getDistributedSync().acquireSync();
      else
         transport.getDistributedSync().releaseSync();
      return null;
   }

   public byte getCommandId() {
      return COMMAND_ID;
   }

   public Object[] getParameters() {
      return new Object[]{enabled};
   }

   public void setParameters(int commandId, Object[] parameters) {
      enabled = (Boolean) parameters[0];
   }

   @Override
   public String toString() {
      return "StateTransferControlCommand{" +
            "enabled=" + enabled +
            '}';
   }
}
