package org.infinispan.client.rest.impl.okhttp.auth;

import org.infinispan.client.rest.configuration.AuthenticationConfiguration;

import okhttp3.Authenticator;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

public class BasicAuthenticator implements CachingAuthenticator {
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
         // We have already attempted to authenticate, fail
         return null;
      }
      String authValue = okhttp3.Credentials.basic(configuration.username(), new String(configuration.password()));
      return request.newBuilder()
            .header("Authorization", authValue)
            .tag(Authenticator.class, this)
            .build();
   }

   @Override
   public Request authenticateWithState(Route route, Request request) {
      return authFromRequest(request);
   }
}
