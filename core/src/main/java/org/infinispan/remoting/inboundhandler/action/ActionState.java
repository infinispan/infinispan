package org.infinispan.remoting.inboundhandler.action;

import org.infinispan.commands.ReplicableCommand;

/**
 * The state used by an {@link Action}.
 * <p/>
 * It is shared among them.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class ActionState {

   private final ReplicableCommand command;
   private final int commandTopologyId;

   public ActionState(ReplicableCommand command, int commandTopologyId) {
      this.command = command;
      this.commandTopologyId = commandTopologyId;
   }

   public final <T extends ReplicableCommand> T getCommand() {
      //noinspection unchecked
      return (T) command;
   }

   public final int getCommandTopologyId() {
      return commandTopologyId;
   }

}
