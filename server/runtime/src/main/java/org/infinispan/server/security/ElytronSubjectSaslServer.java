package org.infinispan.server.security;

import java.security.Principal;
import java.util.List;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.sasl.SaslServer;

import org.infinispan.server.core.security.AuthorizingCallbackHandler;
import org.infinispan.server.core.security.SubjectSaslServer;
import org.wildfly.security.auth.server.SecurityIdentity;
import org.wildfly.security.sasl.WildFlySasl;

/**
 * A {@link SaslServer} which, when complete, can return a negotiated property named {@link #SUBJECT} which contains a
 * populated {@link Subject} representing the authenticated user.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
class ElytronSubjectSaslServer extends SubjectSaslServer {

   public ElytronSubjectSaslServer(SaslServer delegate, List<Principal> principals, AuthorizingCallbackHandler callbackHandler) {
      super(delegate, principals, callbackHandler);
   }

   @Override
   public Object getNegotiatedProperty(String propName) {
      if (SUBJECT.equals(propName)) {
         if (isComplete()) {
            SecurityIdentity identity = (SecurityIdentity) delegate.getNegotiatedProperty(WildFlySasl.SECURITY_IDENTITY);
            Subject subject = new Subject();
            Set<Principal> principals = subject.getPrincipals();
            if (!identity.isAnonymous()) {
               principals.add(identity.getPrincipal());
            }
            identity.getRoles().forEach(role -> principals.add(new RolePrincipal(role)));
            principals.addAll(this.principals);
            return subject;
         } else {
            throw new IllegalStateException("Authentication is not complete");
         }
      } else {
         return delegate.getNegotiatedProperty(propName);
      }
   }
}
