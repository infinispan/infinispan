package org.infinispan.client.rest.impl.jdk.auth;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.configuration.AuthenticationConfiguration;

public class AutoDetectAuthenticator extends HttpAuthenticator {
   private final Map<String, HttpAuthenticator> authenticatorMap;
   // Tracks the authenticator that completed a successful exchange, so we only
   // preauthenticate with the mechanism the server actually supports.
   private volatile HttpAuthenticator activeAuthenticator;

   public AutoDetectAuthenticator(HttpClient client, AuthenticationConfiguration configuration) {
      super(client, configuration);
      authenticatorMap = new LinkedHashMap<>();
      if (configuration.username() != null && configuration.password() != null) {
         authenticatorMap.put("Digest", new DigestAuthenticator(client, configuration));
         authenticatorMap.put("Basic", new BasicAuthenticator(client, configuration));
      }
      if (configuration.username() != null) {
         authenticatorMap.put("Bearer", new BearerAuthenticator(client, configuration));
      }
      authenticatorMap.put("Negotiate", new NegotiateAuthenticator(client, configuration));
      authenticatorMap.put("Localuser", new LocalUserAuthenticator(client, configuration));
   }

   @Override
   public boolean supportsPreauthentication() {
      HttpAuthenticator active = activeAuthenticator;
      return active != null && active.supportsPreauthentication();
   }

   @Override
   public HttpRequest.Builder preauthenticate(HttpRequest.Builder request) {
      HttpAuthenticator active = activeAuthenticator;
      if (active != null && active.supportsPreauthentication()) {
         return active.preauthenticate(request);
      }
      return request;
   }

   @Override
   public <T> CompletionStage<HttpResponse<T>> authenticate(HttpResponse<T> response, HttpResponse.BodyHandler<?> bodyHandler) {
      List<String> headers = response.headers().allValues(WWW_AUTH);
      for (String header : headers) {
         int space = header.indexOf(' ');
         String mech = space >= 0 ? header.substring(0, space) : header;
         HttpAuthenticator authenticator = authenticatorMap.get(mech);
         if (authenticator != null) {
            activeAuthenticator = authenticator;
            return authenticator.authenticate(response, bodyHandler);
         }
      }
      return null;
   }
}
