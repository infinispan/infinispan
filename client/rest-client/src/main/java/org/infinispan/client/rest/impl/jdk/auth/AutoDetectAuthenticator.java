package org.infinispan.client.rest.impl.jdk.auth;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.rest.configuration.AuthenticationConfiguration;
import org.infinispan.client.rest.logging.Log;

public class AutoDetectAuthenticator extends HttpAuthenticator {
   private final Map<String, HttpAuthenticator> authenticatorMap;
   // Tracks the authenticator that completed a successful exchange, so we only pre-authenticate with the mechanism the server actually supports.
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
   public HttpRequest.Builder preauthenticate(HttpRequest.Builder request, AuthRequestState state) {
      HttpAuthenticator active = activeAuthenticator;
      if (active != null && active.supportsPreauthentication()) {
         return active.preauthenticate(request, state);
      }
      return request;
   }

   @Override
   public <T> CompletionStage<HttpResponse<T>> authenticate(HttpResponse<T> response, HttpResponse.BodyHandler<?> bodyHandler, AuthRequestState state) {
      List<String> headers = response.headers().allValues(WWW_AUTH);
      // A fresh authentication exchange starts with a request carrying no credentials at all; reset the per-exchange state.
      if (response.request().headers().firstValue(WWW_AUTH_RESP).isEmpty()) {
         state.challengedMechanisms.clear();
         state.attemptedMechanisms.clear();
      }

      List<String> candidates = new ArrayList<>();
      for (String header : headers) {
         int space = header.indexOf(' ');
         String mech = space >= 0 ? header.substring(0, space) : header;
         state.challengedMechanisms.add(mech);
         if (!candidates.contains(mech)) {
            candidates.add(mech);
         }
      }
      // The server may prune its re-challenges after a mechanism has failed, leaving no advertised alternative (see #18435): also offer mechanisms which were challenged earlier in this exchange but are not listed anymore.
      for (String mech : new ArrayList<>(state.challengedMechanisms)) {
         if (!candidates.contains(mech) && authenticatorMap.containsKey(mech)) {
            candidates.add(mech);
         }
      }

      for (String mech : candidates) {
         HttpAuthenticator authenticator = authenticatorMap.get(mech);
         if (authenticator == null || state.attemptedMechanisms.contains(mech)) {
            continue;
         }
         CompletionStage<HttpResponse<T>> auth = authenticator.authenticate(response, bodyHandler, state);
         if (auth != null) {
            return auth.thenApply(r -> {
               if (r.statusCode() != 401) {
                  activeAuthenticator = authenticator;
                  Log.REST.debugf("Authenticated using %s", mech);
                  state.challengedMechanisms.clear();
                  state.attemptedMechanisms.clear();
               }
               return r;
            });
         } else {
            Log.REST.skippingMechanism(mech);
            // The authenticator declined to attempt (e.g. a previous digest authorization already failed): do not ask it again within this exchange.
            state.attemptedMechanisms.add(mech);
         }
      }
      Log.REST.noSupportedAuthenticationMechanism(headers);
      return null;
   }
}
