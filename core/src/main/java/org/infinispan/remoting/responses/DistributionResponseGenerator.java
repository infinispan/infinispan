package org.infinispan.remoting.responses;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.MapCombineCommand;
import org.infinispan.commands.read.ReduceCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;

/**
 * A response generator for the DIST cache mode
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DistributionResponseGenerator implements ResponseGenerator {
   DistributionManager distributionManager;

   @Inject
   public void inject(DistributionManager distributionManager) {
      this.distributionManager = distributionManager;
   }

   @Override
   public Response getResponse(CacheRpcCommand command, Object returnValue) {
      if (command.getCommandId() == ClusteredGetCommand.COMMAND_ID) {         
         ClusteredGetCommand clusteredGet = (ClusteredGetCommand) command;
         if (returnValue == null && distributionManager.isAffectedByRehash(clusteredGet.getKey()))
            return UnsureResponse.INSTANCE;
         return SuccessfulResponse.create(returnValue);
      } else if (command instanceof SingleRpcCommand) {
         SingleRpcCommand src = (SingleRpcCommand) command;
         ReplicableCommand c = src.getCommand();
         byte commandId = c.getCommandId();
         if (c instanceof WriteCommand) {
            if (returnValue == null) return null;
            // check if this is successful.
            WriteCommand wc = (WriteCommand) c;
            return handleWriteCommand(wc, returnValue);
         } else if (commandId == MapCombineCommand.COMMAND_ID ||
                  commandId == ReduceCommand.COMMAND_ID ||
                  commandId == DistributedExecuteCommand.COMMAND_ID) {
            // Even null values should be wrapped in this case.
            return SuccessfulResponse.create(returnValue);
         } else if (c.isReturnValueExpected()) {
            if (returnValue == null) return null;
            return SuccessfulResponse.create(returnValue);
         }
      } else if (command.isReturnValueExpected()) {
         return SuccessfulResponse.create(returnValue);
      }
      return null; // no unnecessary response values!
   }

   protected Response handleWriteCommand(WriteCommand wc, Object returnValue) {
      return wc.isReturnValueExpected() ? SuccessfulResponse.create(returnValue) : null;
   }
}
