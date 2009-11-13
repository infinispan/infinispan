package org.infinispan.remoting.responses;

import org.infinispan.commands.ReplicableCommand;
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

   public Response getResponse(CacheRpcCommand command, Object returnValue) {
      if (command instanceof ClusteredGetCommand) {
         ClusteredGetCommand clusteredGet = (ClusteredGetCommand) command;
         if (distributionManager.isAffectedByRehash(clusteredGet.getKey()))
            return new UnsureResponse();
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

      return new SuccessfulResponse(returnValue);
   }
}
