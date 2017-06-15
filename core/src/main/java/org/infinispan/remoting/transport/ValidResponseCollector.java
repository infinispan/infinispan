package org.infinispan.remoting.transport;

import org.infinispan.commons.util.Experimental;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.ValidResponse;

/**
 * Base class for response collectors, splitting responses into valid responses, exception responses, and target missing.
 *
 * Returning a non-{@code null} value or throwing an exception from any of the
 * {@link #addValidResponse(Address, ValidResponse)}, {@link #addException(Address, Exception)}, or
 * {@link #addTargetNotFound(Address)} methods will complete the request.
 * If all invocations return {@code null}, the request will be completed with the result of {@link #finish()}.
 *
 * @author Dan Berindei
 * @since 9.1
 */
@Experimental
public abstract class ValidResponseCollector<T> implements ResponseCollector<T> {

   @Override
   public final T addResponse(Address sender, Response response) {
      if (response instanceof ValidResponse) {
         return addValidResponse(sender, ((ValidResponse) response));
      } else if (response instanceof ExceptionResponse) {
         return addException(sender, ((ExceptionResponse) response).getException());
      } else if (response instanceof CacheNotFoundResponse) {
         return addTargetNotFound(sender);
      } else {
         addException(sender, new RpcException("Unknown response type: " + response));
      }
      return null;
   }

   @Override
   public abstract T finish();

   /**
    * Process a valid response from a target.
    *
    * @return {@code null} to continue waiting for response, non-{@code null} to complete with that value.
    */
   protected abstract T addValidResponse(Address sender, ValidResponse response);

   /**
    * Process a target leaving the cluster or stopping the cache.
    *
    * @return {@code null} to continue waiting for response, non-{@code null} to complete with that value.
    */
   protected abstract T addTargetNotFound(Address sender);

   /**
    * Process an exception from a target.
    *
    * @return {@code null} to continue waiting for responses (the default), non-{@code null} to complete with that
    * value.
    */
   protected abstract T addException(Address sender, Exception exception);
}
