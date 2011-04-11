package org.infinispan.remoting.responses;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.recovery.CompleteTransactionCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTransactionsCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTxInfoCommand;

/**
 * The default response generator for most cache modes
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DefaultResponseGenerator implements ResponseGenerator {
   public Response getResponse(CacheRpcCommand command, Object returnValue) {
      if (returnValue == null) return null;
      if (requiresResponse(command.getCommandId())) {
         return new SuccessfulResponse(returnValue);
      } else {
         return null; // saves on serializing a response!
      }
   }

   private boolean requiresResponse(byte commandId) {
      return commandId == ClusteredGetCommand.COMMAND_ID || commandId == GetInDoubtTransactionsCommand.COMMAND_ID
            || commandId == GetInDoubtTxInfoCommand.COMMAND_ID || commandId == CompleteTransactionCommand.COMMAND_ID;
   }
}
