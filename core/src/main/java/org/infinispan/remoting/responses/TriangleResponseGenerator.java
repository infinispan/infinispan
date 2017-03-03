package org.infinispan.remoting.responses;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;

/**
 * A {@link ResponseGenerator} implementation for triangle algorithm.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
public class TriangleResponseGenerator implements ResponseGenerator {

   private static DataWriteCommand extractCommandOrNull(CacheRpcCommand cacheRpcCommand) {
      if (cacheRpcCommand instanceof SingleRpcCommand) {
         ReplicableCommand command = ((SingleRpcCommand) cacheRpcCommand).getCommand();
         return command instanceof PutKeyValueCommand
               || command instanceof RemoveCommand ||
               command instanceof ReplaceCommand ?
               (DataWriteCommand) command : null;
      }
      return null;
   }

   private static boolean isReturnValueExpected(DataWriteCommand dataWriteCommand) {
      return dataWriteCommand.isConditional() || dataWriteCommand.isReturnValueExpected();
   }

   @Override
   public Response getResponse(CacheRpcCommand command, Object returnValue) {
      if (returnValue instanceof Response) {
         return (Response) returnValue;
      }
      //backup owner will receive a BackupCommand.
      DataWriteCommand dataWriteCommand = extractCommandOrNull(command);
      if (dataWriteCommand != null) {
         return new WriteResponse(isReturnValueExpected(dataWriteCommand) ? returnValue : null,
               dataWriteCommand.isSuccessful());
      } else if (command.isReturnValueExpected()) {
         return SuccessfulResponse.create(returnValue);
      } else {
         return null;
      }
   }
}
