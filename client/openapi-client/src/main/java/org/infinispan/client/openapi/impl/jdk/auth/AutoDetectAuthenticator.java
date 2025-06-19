package org.infinispan.client.openapi.impl.jdk.auth;

import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.client.openapi.configuration.AuthenticationConfiguration;


public class AutoDetectAuthenticator extends HttpAuthenticator {
   private final BasicAuthenticator basic;
   private final BearerAuthenticator bearer;
   private final DigestAuthenticator digest;
   private final NegotiateAuthenticator negotiate;

   public AutoDetectAuthenticator(HttpClient client, AuthenticationConfiguration configuration) {
      super(client, configuration);
      basic = new BasicAuthenticator(client, configuration);
      bearer = new BearerAuthenticator(client, configuration);
      digest = new DigestAuthenticator(client, configuration);
      negotiate = new NegotiateAuthenticator(client, configuration);
   }

   @Override
   public <T> CompletionStage<HttpResponse<T>> authenticate(HttpResponse<T> response, HttpResponse.BodyHandler<?> bodyHandler) {
      List<String> headers = response.headers().allValues(WWW_AUTH);
      for (String header : headers) {
         int space = header.indexOf(' ');
         String mech = header.substring(0, space);
         switch (mech) {
            case "Digest":
               return digest.authenticate(response, bodyHandler);
            case "Basic":
               return basic.authenticate(response, bodyHandler);
            case "Bearer":
               return bearer.authenticate(response, bodyHandler);
            case "Negotiate":
               return negotiate.authenticate(response, bodyHandler);
         }
      }
      return null;
   }
}
