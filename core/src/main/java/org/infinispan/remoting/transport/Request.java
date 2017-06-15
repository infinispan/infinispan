package org.infinispan.remoting.transport;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.Experimental;
import org.infinispan.remoting.responses.Response;

/**
 * A remote command invocation request.
 *
 * @author Dan Berindei
 * @since 9.1
 */
@Experimental
public interface Request<T> extends CompletionStage<T> {
   long NO_REQUEST_ID = 0;

   /**
    * @return The unique request id.
    */
   long getRequestId();

   /**
    * Called when a response is received for this response.
    */
   void onResponse(Address sender, Response response);

   /**
    * Called when the node received a new cluster view.
    */
   void onNewView(Set<Address> members);

   /**
    * Complete the request with an exception and release its resources.
    */
   void cancel(Exception cancellationException);
}
