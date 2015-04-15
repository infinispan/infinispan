package org.infinispan.remoting.responses;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.container.versioning.EntryVersionsMap;

/**
 * The default response generator for most cache modes
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class DefaultResponseGenerator implements ResponseGenerator {
   @Override
   public Response getResponse(CacheRpcCommand command, Object returnValue) {
      if (returnValue instanceof Response)
         return (Response) returnValue;

      if (returnValue instanceof EntryVersionsMap || command.isReturnValueExpected()) {
         return SuccessfulResponse.create(returnValue);
      } else {
         return null; // saves on serializing a response!
      }
   }
}
