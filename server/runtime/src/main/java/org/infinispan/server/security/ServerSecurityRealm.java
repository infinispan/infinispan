package org.infinispan.server.security;

import java.util.function.Supplier;

import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.server.core.security.ServerAuthenticationProvider;
import org.wildfly.security.auth.server.SecurityDomain;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ServerSecurityRealm {
   private final String name;
   private final SecurityDomain securityDomain;
   private final Supplier<Boolean> httpChallengeReadiness;

   public ServerSecurityRealm(String name, SecurityDomain securityDomain, Supplier<Boolean> httpChallengeReadiness) {
      this.name = name;
      this.securityDomain = securityDomain;
      this.httpChallengeReadiness = httpChallengeReadiness;
   }

   public String getName() {
      return name;
   }

   public ServerAuthenticationProvider getSASLAuthenticationProvider() {
      return new ElytronSASLAuthenticationProvider(name, securityDomain);
   }

   public Authenticator getHTTPAuthenticationProvider() {
      return new ElytronHTTPAuthenticator(name, this);
   }

   public boolean isReadyForHttpChallenge() {
      return httpChallengeReadiness.get();
   }

   public SecurityDomain getSecurityDomain() {
      return securityDomain;
   }
}
