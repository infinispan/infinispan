package org.infinispan.client.rest.impl.okhttp.auth;

import org.infinispan.client.rest.configuration.AuthenticationConfiguration;

import okhttp3.Authenticator;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

public class BasicAuthenticator extends AbstractAuthenticator implements CachingAuthenticator {
   private final AuthenticationConfiguration configuration;

   public BasicAuthenticator(AuthenticationConfiguration configuration) {
      this.configuration = configuration;
   }

   @Override
   public Request authenticate(Route route, Response response) {
      Request request = response.request();
      return authenticateInternal(request);
   }

   @Override
   public Request authenticateWithState(Route route, Request request) {
      return authenticateInternal(request);
   }

   private Request authenticateInternal(Request request) {
      String authorization = request.header(WWW_AUTH_RESP);
      if (authorization != null && authorization.startsWith("Basic")) {
         // We have already attempted to authenticate, fail
         return null;
      }
      String authValue = okhttp3.Credentials.basic(configuration.username(), new String(configuration.password()));
      return request.newBuilder()
            .header(WWW_AUTH_RESP, authValue)
            .tag(Authenticator.class, this)
            .build();
   }
}
