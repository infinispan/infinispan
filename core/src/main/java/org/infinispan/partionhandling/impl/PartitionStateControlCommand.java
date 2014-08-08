package org.infinispan.partionhandling.impl;

import org.infinispan.commands.remote.BaseRpcCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class PartitionStateControlCommand extends BaseRpcCommand {

   private static final Log log = LogFactory.getLog(PartitionStateControlCommand.class);
   public static final byte COMMAND_ID = 44;

   private PartitionHandlingManager.PartitionState state;
   private PartitionHandlingManager phm;

   private PartitionStateControlCommand() {
      super(null);
   }

   public PartitionStateControlCommand(String ownerCacheName) {
      super(ownerCacheName);
   }

   public PartitionStateControlCommand(String ownerCacheName, PartitionHandlingManager.PartitionState state) {
      super(ownerCacheName);
      this.state = state;
   }

   public void init(PartitionHandlingManager phm) {
      this.phm = phm;
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      phm.setState(state);
      return null;
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return new Object[]{state};
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Invalid method id " + commandId + " but "
                                               + this.getClass() + " has id " + getCommandId());
      state = (PartitionHandlingManager.PartitionState) parameters[0];
   }

   @Override
   public boolean isReturnValueExpected() {
      return false;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PartitionStateControlCommand that = (PartitionStateControlCommand) o;

      if (state != that.state) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return state.hashCode();
   }

   @Override
   public String toString() {
      return "PartitionStateControlCommand [state=" + state + "]";
   }
}
