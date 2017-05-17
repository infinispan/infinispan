package org.infinispan.rest.authentication;

import java.util.Optional;

import org.infinispan.rest.InfinispanRequest;
import org.infinispan.rest.InfinispanResponse;
import org.infinispan.rest.RestResponseException;

import io.netty.handler.codec.http.HttpResponseStatus;

/**
 * Exception thrown then authentication fails.
 */
public class AuthenticationException extends RestResponseException {

   private Optional<String> authenticationHeader;

   /**
    * Creates new {@link AuthenticationException}.
    *
    * @param authenticationHeader Authentication header which will be sent to the client.
    */
   public AuthenticationException(Optional<String> authenticationHeader) {
      super(HttpResponseStatus.UNAUTHORIZED, null);
      this.authenticationHeader = authenticationHeader;
   }

   @Override
   public InfinispanResponse toResponse(InfinispanRequest request) {
      InfinispanResponse response = super.toResponse(request);
      authenticationHeader.ifPresent(header -> response.authenticate(header));
      return response;
   }
}
