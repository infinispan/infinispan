package org.infinispan.rest.authentication;

import java.util.Optional;

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

   public String getAuthenticationHeader() {
      return authenticationHeader.orElse(null);
   }

}
