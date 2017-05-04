package org.infinispan.remoting.responses;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * The default response generator for most cache modes
 *
 * @author Manik Surtani
 * @author Dan Berindei
 * @since 4.0
 */
public class DefaultResponseGenerator implements ResponseGenerator {
   private static final Log log = LogFactory.getLog(DefaultResponseGenerator.class);
   private static final boolean trace = log.isTraceEnabled();

   public Response getResponse(CacheRpcCommand command, Object returnValue) {
      if (returnValue instanceof Response)
         return (Response) returnValue;

      if (command.isReturnValueExpected()) {
         return command.isSuccessful() ? SuccessfulResponse.create(returnValue) : UnsuccessfulResponse.create(returnValue);
      } else {
         if (returnValue != null) {
            if (trace) log.tracef("Ignoring non-null response for command %s: %s", command, returnValue);
         }
         return null; // saves on serializing a response!
      }
   }
}
