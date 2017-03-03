package org.infinispan.server.endpoint.subsystem.security;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

import java.io.IOException;
import java.security.Principal;
import java.util.Collections;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;

import org.infinispan.security.Security;
import org.infinispan.server.core.security.simple.SimpleUserPrincipal;
import org.jboss.as.core.security.SubjectUserInfo;
import org.jboss.as.domain.management.AuthMechanism;
import org.jboss.as.domain.management.AuthorizingCallbackHandler;
import org.jboss.as.domain.management.SecurityRealm;
import org.jboss.resteasy.plugins.server.embedded.SecurityDomain;
import org.jboss.sasl.callback.VerifyPasswordCallback;

/**
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
public class BasicRestSecurityDomain implements SecurityDomain {
   private final SecurityRealm securityRealm;

   public BasicRestSecurityDomain(SecurityRealm securityRealm) {
      this.securityRealm = securityRealm;
   }

   @Override
   public Principal authenticate(String username, String password) throws SecurityException {
      AuthorizingCallbackHandler handler = securityRealm.getAuthorizingCallbackHandler(AuthMechanism.PLAIN);
      NameCallback ncb = new NameCallback("name", username);
      ncb.setName(username);
      VerifyPasswordCallback vpcb = new VerifyPasswordCallback(password);
      try {
         handler.handle(new Callback[] { ncb, vpcb });
      } catch (Exception e) {
         ROOT_LOGGER.authenticationError(e);
      }
      if (vpcb.isVerified()) {
         try {
            SubjectUserInfo subjectUserInfo = handler.createSubjectUserInfo(Collections.singletonList(new SimpleUserPrincipal(username)));
            return Security.getSubjectUserPrincipal(subjectUserInfo.getSubject());
         } catch (IOException e) {
            throw new SecurityException("Invalid credentials", e);
         }

      } else throw new SecurityException("Invalid credentials");
   }

   @Override
   public boolean isUserInRole(Principal principal, String role) {
      return true;
   }
}
