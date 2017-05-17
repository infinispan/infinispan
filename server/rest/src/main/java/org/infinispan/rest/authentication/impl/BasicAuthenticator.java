package org.infinispan.rest.authentication.impl;

import java.util.Base64;
import java.util.Optional;

import org.infinispan.rest.InfinispanRequest;
import org.infinispan.rest.authentication.AuthenticationException;
import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.rest.authentication.SecurityDomain;

/**
 * BASIC authentication mechanism.
 *
 * @author Tristan Tarrant
 * @author Sebastian Łaskawiec
 */
public class BasicAuthenticator implements Authenticator {

   private final SecurityDomain domain;
   private final String authenticateHeader;

   public BasicAuthenticator(SecurityDomain domain, String realm) {
      this.domain = domain;
      this.authenticateHeader = realm != null ? String.format("Basic realm=\"%s\"", realm) : "Basic";
   }

   @Override
   public void challenge(InfinispanRequest request) throws AuthenticationException {
      String auth = request.getAuthorization().orElseThrow(() -> new AuthenticationException(Optional.of(authenticateHeader)));
      if (auth.length() > 5) {
         String type = auth.substring(0, 5);
         type = type.toLowerCase();
         if ("basic".equals(type)) {
            String cookie = auth.substring(6);
            cookie = new String(Base64.getDecoder().decode(cookie.getBytes()));
            String[] split = cookie.split(":");
            try {
               domain.authenticate(split[0], split[1]);
               //authenticated
               return;
            } catch (SecurityException e) {
            }
         }
      }
      throw new AuthenticationException(Optional.of(authenticateHeader));
   }
}
