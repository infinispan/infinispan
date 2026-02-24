package org.infinispan.client.rest.impl.jdk.auth;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.configuration.AuthenticationConfiguration;

public class BasicAuthenticator extends HttpAuthenticator {
   private final String authzValue;

   public BasicAuthenticator(HttpClient client, AuthenticationConfiguration configuration) {
      super(client, configuration);
      authzValue = (configuration.username() != null && configuration.password() != null) ? basic(configuration.username(), new String(configuration.password())) : null;
   }

   @Override
   public boolean supportsPreauthentication() {
      return authzValue != null;
   }

   @Override
   public HttpRequest.Builder preauthenticate(HttpRequest.Builder request) {
      return request.header(WWW_AUTH_RESP, authzValue);
   }

   @Override
   public <T> CompletionStage<HttpResponse<T>> authenticate(HttpResponse<T> response, HttpResponse.BodyHandler<?> bodyHandler) {
      HttpRequest request = response.request();
      List<String> authorization = request.headers().allValues(WWW_AUTH_RESP);
      if (authzValue == null || !authorization.isEmpty() && authorization.get(0).startsWith("Basic")) {
         // We have already attempted to authenticate, fail
         return null;
      }
      HttpRequest.Builder newRequest = copyRequest(request, (n, v) -> true).header(WWW_AUTH_RESP, authzValue);
      return client.sendAsync(newRequest.build(), (HttpResponse.BodyHandler<T>) bodyHandler);
   }

   public static String basic(String username, String password) {
      String usernameAndPassword = username + ":" + password;
      String encoded = Base64.getEncoder().encodeToString(usernameAndPassword.getBytes(StandardCharsets.ISO_8859_1));
      return "Basic " + encoded;
   }
}
