package org.infinispan.server.security;

import java.io.OutputStream;

import org.infinispan.rest.NettyRestResponse;
import org.infinispan.rest.RestResponseException;
import org.wildfly.security.http.HttpAuthenticationException;
import org.wildfly.security.http.HttpServerCookie;
import org.wildfly.security.http.HttpServerMechanismsResponder;
import org.wildfly.security.http.HttpServerResponse;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class HttpServerResponseAdapter implements HttpServerResponse {
   private final NettyRestResponse.Builder builder;

   private HttpServerResponseAdapter(NettyRestResponse.Builder responseBuilder) {
      this.builder = responseBuilder;
   }

   public static void adapt(HttpServerMechanismsResponder responder, NettyRestResponse.Builder responseBuilder) {
      try {
         HttpServerResponseAdapter response = new HttpServerResponseAdapter(responseBuilder);
         if (responder != null) {
            responder.sendResponse(response);
         }
         response.builder.build();
      } catch (HttpAuthenticationException e) {
         throw new RestResponseException(e);
      }
   }

   @Override
   public void addResponseHeader(String headerName, String headerValue) {
      builder.header(headerName, headerValue);
   }

   @Override
   public void setStatusCode(int statusCode) {
      builder.status(statusCode);
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
      builder.header("Set-Cookie", value.toString());
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
