package org.infinispan.remoting.transport;

import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.UnsureResponse;

/**
 * Return the first response that is either successful or exceptional.
 *
 * Return {@link UnsureResponse} if there are no acceptable responses but at least one unsure response,
 * and {@link CacheNotFoundResponse} if there are no successful, exceptional, or unsure responses.
 *
 * @author Dan Berindei
 * @since 9.2
 */
public class RemoteGetResponseCollector implements ResponseCollector<Response> {
   private boolean hasUnsureResponse;

   @Override
   public Response addResponse(Address sender, Response response) {
      if (response.isSuccessful()) {
         return response;
      }
      if (response instanceof ExceptionResponse) {
         throw ResponseCollectors.wrapRemoteException(sender, ((ExceptionResponse) response).getException());
      }

      if (response instanceof UnsureResponse) {
         hasUnsureResponse = true;
      }
      return null;
   }

   @Override
   public Response finish() {
      return hasUnsureResponse ? UnsureResponse.INSTANCE : CacheNotFoundResponse.INSTANCE;
   }
}
