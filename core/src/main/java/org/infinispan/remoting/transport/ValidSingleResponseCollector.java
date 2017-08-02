package org.infinispan.remoting.transport;

import org.infinispan.commons.util.Experimental;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;

/**
 * @author Dan Berindei
 * @since 9.1
 */
@Experimental
public abstract class ValidSingleResponseCollector<T> implements ResponseCollector<T> {
   @Override
   public final T addResponse(Address sender, Response response) {
      if (response instanceof ValidResponse) {
         return withValidResponse(sender, ((ValidResponse) response));
      } else if (response instanceof ExceptionResponse) {
         return withException(sender, ((ExceptionResponse) response).getException());
      } else if (response instanceof CacheNotFoundResponse) {
         return targetNotFound(sender);
      } else {
         // Should never happen
         return withException(sender, new RpcException("Unknown response type: " + response));
      }
   }

   @Override
   public final T finish() {
      // addResponse returned null, that means we want the final result to be null.
      return null;
   }

   protected T withException(Address sender, Exception exception) {
      throw ResponseCollectors.wrapRemoteException(sender, exception);
   }

   protected abstract T withValidResponse(Address sender, ValidResponse response);

   protected abstract T targetNotFound(Address sender);

}
