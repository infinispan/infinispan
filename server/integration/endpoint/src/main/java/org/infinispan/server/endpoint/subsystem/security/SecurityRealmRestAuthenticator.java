package org.infinispan.server.endpoint.subsystem.security;

import static org.wildfly.security.http.HttpConstants.SECURITY_IDENTITY;

import java.util.Optional;

import javax.security.auth.Subject;

import org.infinispan.rest.InfinispanRequest;
import org.infinispan.rest.RestResponseException;
import org.infinispan.rest.authentication.AuthenticationException;
import org.infinispan.rest.authentication.Authenticator;
import org.jboss.as.core.security.RealmRole;
import org.wildfly.security.auth.server.HttpAuthenticationFactory;
import org.wildfly.security.auth.server.SecurityIdentity;
import org.wildfly.security.http.HttpAuthenticationException;
import org.wildfly.security.http.HttpServerAuthenticationMechanism;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class SecurityRealmRestAuthenticator implements Authenticator {
   private final HttpAuthenticationFactory factory;
   private final String mechanismName;

   public SecurityRealmRestAuthenticator(HttpAuthenticationFactory factory, String mechanism) {
      this.factory = factory;
      this.mechanismName = mechanism;
   }

   @Override
   public void challenge(InfinispanRequest request) throws RestResponseException {
      HttpServerRequestAdapter requestAdapter = new HttpServerRequestAdapter(request);
      try {
         HttpServerAuthenticationMechanism mechanism = factory.createMechanism(this.mechanismName);
         mechanism.evaluateRequest(requestAdapter);
         requestAdapter.validateResponse();
         SecurityIdentity securityIdentity = (SecurityIdentity) mechanism.getNegotiatedProperty(SECURITY_IDENTITY);
         if (securityIdentity != null) {
            Subject subject = new Subject();
            subject.getPrincipals().add(securityIdentity.getPrincipal());
            securityIdentity.getRoles().forEach(r ->  subject.getPrincipals().add(new RealmRole(r)));
            request.setSubject(subject);
         }
      } catch (HttpAuthenticationException e) {
         throw new AuthenticationException(Optional.empty());
      }
   }
}
