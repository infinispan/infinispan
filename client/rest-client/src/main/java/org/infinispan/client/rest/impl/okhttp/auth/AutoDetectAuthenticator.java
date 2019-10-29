package org.infinispan.client.rest.impl.okhttp.auth;

import java.io.IOException;
import java.util.List;

import org.infinispan.client.rest.configuration.AuthenticationConfiguration;

import okhttp3.Request;
import okhttp3.Response;
import okhttp3.Route;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class AutoDetectAuthenticator extends AbstractAuthenticator implements CachingAuthenticator {

   private final BasicAuthenticator basic;
   private final BearerAuthenticator bearer;
   private final DigestAuthenticator digest;
   private final NegotiateAuthenticator negotiate;

   public AutoDetectAuthenticator(AuthenticationConfiguration configuration) {
      basic = new BasicAuthenticator(configuration);
      bearer = new BearerAuthenticator(configuration);
      digest = new DigestAuthenticator(configuration);
      negotiate = new NegotiateAuthenticator(configuration);
   }

   @Override
   public Request authenticate(Route route, Response response) throws IOException {
      List<String> headers = response.headers(WWW_AUTH);
      for (String header : headers) {
         int space = header.indexOf(' ');
         String mech = header.substring(0, space);
         switch (mech) {
            case "Digest":
               return digest.authenticate(route, response);
            case "Basic":
               return basic.authenticate(route, response);
            case "Bearer":
               return bearer.authenticate(route, response);
            case "Negotiate":
               return negotiate.authenticate(route, response);
         }
      }
      return null;
   }

   @Override
   public Request authenticateWithState(Route route, Request request) throws IOException {
      // This should never get called
      throw new IllegalStateException();
   }

}
