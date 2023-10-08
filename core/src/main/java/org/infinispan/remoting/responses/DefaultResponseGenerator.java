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

   public Response getResponse(CacheRpcCommand command, Object rv) {
      if (rv instanceof Response)
         return (Response) rv;

      if (command.isReturnValueExpected()) {
         if (!command.isSuccessful())
            return rv == null ? UnsuccessfulResponse.EMPTY_RESPONSE : new UnsuccessfulResponse<>(rv);

         return SuccessfulResponse.create(rv);
      } else {
         if (rv != null) {
            if (log.isTraceEnabled()) log.tracef("Ignoring non-null response for command %s: %s", command, rv);
         }
         return null; // saves on serializing a response!
      }
   }
}
