package org.infinispan.server.endpoint.subsystem.security;

import java.security.Principal;

import org.jboss.as.domain.management.SecurityRealm;
import org.jboss.resteasy.plugins.server.embedded.SecurityDomain;

/**
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class ClientCertRestSecurityDomain implements SecurityDomain {
   private final SecurityRealm securityRealm;
   private static final Principal CLIENT_CERT_PRINCIPAL = () -> "CLIENT_CERTIFICATE";

   public ClientCertRestSecurityDomain(SecurityRealm securityRealm) {
      this.securityRealm = securityRealm;
   }

   @Override
   public Principal authenticate(String username, String password) throws SecurityException {
      // RestEasy's SecurityDomain doesn't really allow us to authenticate by client cert, so just fake it here
      return CLIENT_CERT_PRINCIPAL;
   }

   @Override
   public boolean isUserInRole(Principal principal, String role) {
      return true;
   }
}
