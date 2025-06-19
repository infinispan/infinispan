package org.infinispan.client.openapi.impl.jdk.auth;

/**
 * @since 15.0
 **/
public class AuthenticationException extends SecurityException {
   public AuthenticationException(String s) {
      super(s);
   }

   public AuthenticationException(String message, Exception ex) {
      super(message, ex);
   }
}
