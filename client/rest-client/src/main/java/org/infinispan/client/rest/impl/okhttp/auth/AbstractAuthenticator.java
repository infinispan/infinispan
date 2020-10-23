package org.infinispan.client.rest.impl.okhttp.auth;

import java.util.List;

import okhttp3.Headers;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.1
 **/
public abstract class AbstractAuthenticator {
   public static final String WWW_AUTH = "WWW-Authenticate";
   public static final String WWW_AUTH_RESP = "Authorization";

   static String findHeader(Headers headers, String name, String prefix) {
      final List<String> authHeaders = headers.values(name);
      for (String header : authHeaders) {
         if (header.startsWith(prefix)) {
            return header;
         }
      }
      throw new AuthenticationException("unsupported auth scheme: " + authHeaders);
   }

   public static class AuthenticationException extends SecurityException {
      public AuthenticationException(String s) {
         super(s);
      }

      public AuthenticationException(String message, Exception ex) {
         super(message, ex);
      }
   }
}
