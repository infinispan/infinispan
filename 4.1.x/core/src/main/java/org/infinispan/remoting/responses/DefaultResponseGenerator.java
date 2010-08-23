package org.infinispan.remoting.responses;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;

/**
 * The default response generator for most cache modes
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DefaultResponseGenerator implements ResponseGenerator {
   public Response getResponse(CacheRpcCommand command, Object returnValue) {
      if (returnValue != null && command instanceof ClusteredGetCommand)
         return new SuccessfulResponse(returnValue);
      else
         return null; // saves on serializing a response!
   }
}
