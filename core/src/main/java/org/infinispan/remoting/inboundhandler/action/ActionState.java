package org.infinispan.remoting.inboundhandler.action;

import java.util.List;

import org.infinispan.commands.ReplicableCommand;

/**
 * The state used by an {@link Action}.
 * <p>
 * It is shared among them.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
public class ActionState {

   private final ReplicableCommand command;
   private final int commandTopologyId;
   private volatile long timeout;
   private volatile List<Object> filteredKeys;

   public ActionState(ReplicableCommand command, int commandTopologyId, long timeout) {
      this.command = command;
      this.commandTopologyId = commandTopologyId;
      this.timeout = timeout;
   }

   public final <T extends ReplicableCommand> T getCommand() {
      //noinspection unchecked
      return (T) command;
   }

   public final int getCommandTopologyId() {
      return commandTopologyId;
   }

   public final long getTimeout() {
      return timeout;
   }

   public final void updateTimeout(long newTimeout) {
      this.timeout = newTimeout;
   }

   public final List<Object> getFilteredKeys() {
      return filteredKeys;
   }

   public final void updateFilteredKeys(List<Object> newFilteredKeys) {
      this.filteredKeys = newFilteredKeys;
   }
}
