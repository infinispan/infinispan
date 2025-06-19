package org.infinispan.client.openapi.impl.jdk.auth;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.openapi.configuration.AuthenticationConfiguration;


public class BearerAuthenticator extends HttpAuthenticator {
   private final String authzValue;

   public BearerAuthenticator(HttpClient client, AuthenticationConfiguration configuration) {
      super(client, configuration);
      authzValue = "Bearer " + configuration.username();
   }

   @Override
   public boolean supportsPreauthentication() {
      return true;
   }

   @Override
   public HttpRequest.Builder preauthenticate(HttpRequest.Builder request) {
      return request.header(WWW_AUTH_RESP, authzValue);
   }

   @Override
   public <T> CompletionStage<HttpResponse<T>> authenticate(HttpResponse<T> response, HttpResponse.BodyHandler<?> bodyHandler) {
      HttpRequest request = response.request();
      List<String> authorization = request.headers().allValues(WWW_AUTH_RESP);
      if (!authorization.isEmpty() && authorization.get(0).startsWith("Bearer")) {
         // We have already attempted to authenticate, fail
         return null;
      }
      HttpRequest.Builder newRequest = copyRequest(request, (n, v) -> true).header(WWW_AUTH_RESP, authzValue);
      return client.sendAsync(newRequest.build(), (HttpResponse.BodyHandler<T>) bodyHandler);
   }
}
