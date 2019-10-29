package org.infinispan.server.security;

import java.util.function.Supplier;

import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.server.configuration.security.ServerIdentitiesConfiguration;
import org.infinispan.server.core.security.ServerAuthenticationProvider;
import org.wildfly.security.auth.server.MechanismConfiguration;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.credential.source.CredentialSource;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class ServerSecurityRealm {
   private final String name;
   private final SecurityDomain securityDomain;
   private final Supplier<Boolean> httpChallengeReadiness;
   private final ServerIdentitiesConfiguration serverIdentities;

   public ServerSecurityRealm(String name, SecurityDomain securityDomain, Supplier<Boolean> httpChallengeReadiness, ServerIdentitiesConfiguration serverIdentities) {
      this.name = name;
      this.securityDomain = securityDomain;
      this.httpChallengeReadiness = httpChallengeReadiness;
      this.serverIdentities = serverIdentities;
   }

   public String getName() {
      return name;
   }

   public ServerAuthenticationProvider getSASLAuthenticationProvider(String serverPrincipal) {
      return new ElytronSASLAuthenticationProvider(name, this, serverPrincipal);
   }

   public Authenticator getHTTPAuthenticationProvider(String serverPrincipal) {
      return new ElytronHTTPAuthenticator(name, this, serverPrincipal);
   }

   public boolean isReadyForHttpChallenge() {
      return httpChallengeReadiness.get();
   }

   public SecurityDomain getSecurityDomain() {
      return securityDomain;
   }

   public void applyServerCredentials(MechanismConfiguration.Builder mechConfigurationBuilder, String serverPrincipal) {
      if (serverPrincipal != null) {
         CredentialSource credentialSource = serverIdentities.getCredentialSource(serverPrincipal);
         mechConfigurationBuilder.setServerCredentialSource(credentialSource);
      }
   }
}
