package org.infinispan.remoting.rpc;

import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.transport.Address;

/**
 * A mechanism of filtering RPC responses.  Used with the RPC manager.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface ResponseFilter {
   /**
    * Determines whether a response from a given sender should be added to the response list of the request
    *
    * @param response The response (usually a serializable value)
    * @param sender   The sender of response
    * @return True if we should add the response to the response list of a request, otherwise false. In the latter case,
    *         we don't add the response to the response list.
    */
   boolean isAcceptable(Response response, Address sender);

   /**
    * Right after calling {@link #isAcceptable(Response, Address)}, this method is called to see whether we are done
    * with the request and can unblock the caller
    *
    * @return False if the request is done, otherwise true
    */
   boolean needMoreResponses();

}
