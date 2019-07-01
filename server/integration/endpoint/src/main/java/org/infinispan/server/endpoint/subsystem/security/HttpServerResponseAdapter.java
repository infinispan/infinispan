package org.infinispan.server.endpoint.subsystem.security;

import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.rest.InfinispanErrorResponse;
import org.infinispan.rest.InfinispanResponse;
import org.wildfly.security.http.HttpAuthenticationException;
import org.wildfly.security.http.HttpServerCookie;
import org.wildfly.security.http.HttpServerMechanismsResponder;
import org.wildfly.security.http.HttpServerResponse;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class HttpServerResponseAdapter implements HttpServerResponse {
   private int status = 0;
   private Map<String, String> headers = new HashMap<>(2);

   private HttpServerResponseAdapter() {
   }

   public static InfinispanResponse getResponse(HttpServerMechanismsResponder responder) {
      try {
         if (responder != null) {
            HttpServerResponseAdapter responseAdapter = new HttpServerResponseAdapter();
            responder.sendResponse(responseAdapter);
            if (responseAdapter.status == 401) {
               return InfinispanErrorResponse.unauthorized(responseAdapter.headers.get("www-authenticate"));
            } else {
               return null;
            }
         }
         return null;
      } catch (HttpAuthenticationException e) {
         throw new RuntimeException(e);
      }
   }

   @Override
   public void addResponseHeader(String headerName, String headerValue) {
      headers.put(headerName.toLowerCase(), headerValue);
   }

   @Override
   public void setStatusCode(int statusCode) {
      this.status = statusCode;
   }

   @Override
   public void setResponseCookie(HttpServerCookie cookie) {
      StringBuilder value = new StringBuilder();
      value.append(cookie.getName());
      value.append('=');
      value.append(cookie.getValue());
      if (cookie.isHttpOnly()) {
         value.append("; HttpOnly");
      }
      if (cookie.isSecure()) {
         value.append("; Secure");
      }
      if (cookie.getDomain() != null) {
         value.append("; Domain=").append(cookie.getDomain());
      }
      if (cookie.getPath() != null) {
         value.append("; Path=").append(cookie.getPath());
      }
      headers.put("Set-Cookie", value.toString());
   }

   @Override
   public OutputStream getOutputStream() {
      return null;
   }

   @Override
   public boolean forward(String path) {
      return false;
   }
}
