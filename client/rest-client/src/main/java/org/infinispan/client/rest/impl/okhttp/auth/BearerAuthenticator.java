package org.infinispan.client.rest.impl.okhttp.auth;

import org.infinispan.client.rest.configuration.AuthenticationConfiguration;

import okhttp3.Authenticator;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

public class BearerAuthenticator implements CachingAuthenticator {
   private final AuthenticationConfiguration configuration;

   public BearerAuthenticator(AuthenticationConfiguration configuration) {
      this.configuration = configuration;
   }

   @Override
   public Request authenticate(Route route, Response response) {
      final Request request = response.request();
      return authFromRequest(request);
   }

   private Request authFromRequest(Request request) {
      final String authorization = request.header("Authorization");
      if (authorization != null && authorization.startsWith("Bearer")) {
         // We have already attempted to authenticate, fail
         return null;
      }
      return request.newBuilder()
            .header("Authorization", "Bearer " + configuration.username())
            .tag(Authenticator.class, this)
            .build();
   }

   @Override
   public Request authenticateWithState(Route route, Request request) {
      return authFromRequest(request);
   }
}
