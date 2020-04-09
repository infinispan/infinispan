package org.infinispan.rest.resources.security;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.net.URI;
import java.util.Base64;

import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.http.HttpHeader;

/**
 * Http client that sends Basic authentication headers for every request.
 */
public class AuthClient extends HttpClient {
   private final String credentials;

   public AuthClient(String user, String pass) {
      this.credentials = Base64.getEncoder().encodeToString(String.format("%s:%s", user, pass).getBytes(UTF_8));
   }

   @Override
   public Request newRequest(String host, int port) {
      return addAuthHeader(super.newRequest(host, port));
   }

   @Override
   public Request newRequest(String uri) {
      return addAuthHeader(super.newRequest(uri));
   }

   @Override
   public Request newRequest(URI uri) {
      return addAuthHeader(super.newRequest(uri));
   }

   private Request addAuthHeader(Request request) {
      request.header(HttpHeader.AUTHORIZATION, "Basic " + credentials);
      return request;
   }
}
