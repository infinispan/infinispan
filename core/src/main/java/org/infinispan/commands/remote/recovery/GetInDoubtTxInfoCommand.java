package org.infinispan.commands.remote.recovery;

import org.infinispan.commons.util.Util;
import org.infinispan.context.InvocationContext;

/**
 * Command used by the recovery tooling for obtaining the list of in-doubt transactions from a node.
 *
 * @author Mircea Markus
 * @since 5.0
 */
public class GetInDoubtTxInfoCommand extends RecoveryCommand {

   public static final int COMMAND_ID = 23;

   private GetInDoubtTxInfoCommand() {
      super(null); // For command id uniqueness test
   }

   public GetInDoubtTxInfoCommand(String cacheName) {
      super(cacheName);
   }

   @Override
   public Object perform(InvocationContext ctx) throws Throwable {
      return recoveryManager.getInDoubtTransactionInfo();
   }

   @Override
   public byte getCommandId() {
      return COMMAND_ID;
   }

   @Override
   public Object[] getParameters() {
      return Util.EMPTY_OBJECT_ARRAY;
   }

   @Override
   public void setParameters(int commandId, Object[] parameters) {
      if (commandId != COMMAND_ID)
         throw new IllegalStateException("Expected " + COMMAND_ID + "and received " + commandId);
      // No parameters
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + " { cacheName = " + cacheName + "}";
   }

}
