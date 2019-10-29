package org.infinispan.client.rest.impl.okhttp.auth;

import org.infinispan.client.rest.configuration.AuthenticationConfiguration;

import okhttp3.Authenticator;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

public class BearerAuthenticator extends AbstractAuthenticator implements CachingAuthenticator {
   private final AuthenticationConfiguration configuration;

   public BearerAuthenticator(AuthenticationConfiguration configuration) {
      this.configuration = configuration;
   }

   @Override
   public Request authenticate(Route route, Response response) {
      final Request request = response.request();
      return authenticateInternal(request);
   }

   @Override
   public Request authenticateWithState(Route route, Request request) {
      return authenticateInternal(request);
   }

   private Request authenticateInternal(Request request) {
      final String authorization = request.header(WWW_AUTH_RESP);
      if (authorization != null && authorization.startsWith("Bearer")) {
         // We have already attempted to authenticate, fail
         return null;
      }
      return request.newBuilder()
            .header(WWW_AUTH_RESP, "Bearer " + configuration.username())
            .tag(Authenticator.class, this)
            .build();
   }


}
