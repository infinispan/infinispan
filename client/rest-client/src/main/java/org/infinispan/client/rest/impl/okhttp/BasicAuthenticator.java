package org.infinispan.client.rest.impl.okhttp;

import org.infinispan.client.rest.configuration.AuthenticationConfiguration;

import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

public class BasicAuthenticator implements StatefulAuthenticator {
   private final AuthenticationConfiguration configuration;

   public BasicAuthenticator(AuthenticationConfiguration configuration) {
      this.configuration = configuration;
   }

   @Override
   public Request authenticate(Route route, Response response) {
      final Request request = response.request();
      return authFromRequest(request);
   }

   private Request authFromRequest(Request request) {
      final String authorization = request.header("Authorization");
      if (authorization != null && authorization.startsWith("Basic")) {
         return null;
      }
      String authValue = okhttp3.Credentials.basic(configuration.username(), new String(configuration.password()));
      return request.newBuilder()
            .header("Authorization", authValue)
            .build();
   }

   @Override
   public Request authenticateWithState(Route route, Request request) {
      return authFromRequest(request);
   }
}
