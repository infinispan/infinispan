package org.infinispan.server.endpoint.subsystem.security;

import static org.infinispan.server.endpoint.EndpointLogger.ROOT_LOGGER;

import java.io.IOException;
import java.security.Principal;
import java.util.Collections;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;

import org.infinispan.rest.authentication.SecurityDomain;
import org.infinispan.security.Security;
import org.infinispan.server.core.security.simple.SimpleUserPrincipal;
import org.jboss.as.core.security.SubjectUserInfo;
import org.jboss.as.domain.management.AuthMechanism;
import org.jboss.as.domain.management.AuthorizingCallbackHandler;
import org.jboss.as.domain.management.SecurityRealm;
import org.wildfly.security.auth.callback.EvidenceVerifyCallback;
import org.wildfly.security.evidence.PasswordGuessEvidence;

public class BasicRestSecurityDomain implements SecurityDomain {

   private final SecurityRealm securityRealm;

   public BasicRestSecurityDomain(SecurityRealm securityRealm) {
      this.securityRealm = securityRealm;
   }

   public Principal authenticate(String username, String password) throws SecurityException {
      AuthorizingCallbackHandler handler = securityRealm.getAuthorizingCallbackHandler(AuthMechanism.PLAIN);
      NameCallback ncb = new NameCallback("name", username);
      ncb.setName(username);
      EvidenceVerifyCallback evcb = new EvidenceVerifyCallback(new PasswordGuessEvidence(password.toCharArray()));
      try {
         handler.handle(new Callback[] { ncb, evcb });
      } catch (Exception e) {
         ROOT_LOGGER.authenticationError(e);
      }
      if (evcb.isVerified()) {
         try {
            SubjectUserInfo subjectUserInfo = handler.createSubjectUserInfo(Collections.singletonList(new SimpleUserPrincipal(username)));
            return Security.getSubjectUserPrincipal(subjectUserInfo.getSubject());
         } catch (IOException e) {
            throw new SecurityException("Invalid credentials", e);
         }

      } else throw new SecurityException("Invalid credentials");
   }
}
