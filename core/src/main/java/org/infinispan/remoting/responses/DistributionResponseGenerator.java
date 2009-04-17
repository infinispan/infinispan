package org.infinispan.remoting.responses;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.write.WriteCommand;

/**
 * A response generator for the DIST cache mode
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DistributionResponseGenerator implements ResponseGenerator {
   public Response getResponse(CacheRpcCommand command, Object returnValue) {
      if (command instanceof ClusteredGetCommand) {
         return returnValue == null ? null : new SuccessfulResponse(returnValue);
      } else if (command instanceof SingleRpcCommand) {
         SingleRpcCommand src = (SingleRpcCommand) command;
         ReplicableCommand c = src.getCommand();

         if (c instanceof WriteCommand) {
            // check if this is successful.
            if (((WriteCommand) c).isSuccessful())
               return new SuccessfulResponse(returnValue);
            else
               return UnsuccessfulResponse.INSTANCE;
         }
      }

      if (returnValue == null)
         return UnsuccessfulResponse.INSTANCE;
      else
         return new SuccessfulResponse(returnValue);
   }
}
