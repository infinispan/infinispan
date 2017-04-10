package org.infinispan.rest.server.authentication.exceptions;

import java.util.Optional;

import org.infinispan.rest.server.InfinispanResponse;
import org.infinispan.rest.server.RestResponseException;

import io.netty.handler.codec.http.HttpResponseStatus;

public class AuthenticationException extends RestResponseException {

   private Optional<String> authenticationHeader;

   public AuthenticationException(Optional<String> authenticationHeader) {
      super(HttpResponseStatus.UNAUTHORIZED, "Authentication failed");
      this.authenticationHeader = authenticationHeader;
   }


   @Override
   public InfinispanResponse toResponse() {
      InfinispanResponse response = super.toResponse();
      authenticationHeader.ifPresent(header -> response.authenticate(header));
      return response;
   }
}
