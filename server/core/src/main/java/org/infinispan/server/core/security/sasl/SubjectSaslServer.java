package org.infinispan.server.core.security.sasl;

import java.security.Principal;
import java.util.ArrayList;
import java.util.List;

import javax.security.auth.Subject;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.infinispan.server.core.security.UserPrincipal;
import org.infinispan.server.core.security.simple.SimpleUserPrincipal;

/**
 * A {@link SaslServer} which, when complete, can return a negotiated property named {@link #SUBJECT} which contains a
 * populated {@link Subject} representing the authenticated user.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class SubjectSaslServer implements SaslServer {
   public static final String SUBJECT = "org.infinispan.security.Subject";
   protected final SaslServer delegate;
   protected final AuthorizingCallbackHandler callbackHandler;
   protected final List<Principal> principals;

   public SubjectSaslServer(SaslServer delegate, List<Principal> principals, AuthorizingCallbackHandler callbackHandler) {
      this.delegate = delegate;
      this.principals = principals;
      this.callbackHandler = callbackHandler;
   }

   @Override
   public String getMechanismName() {
      return delegate.getMechanismName();
   }

   @Override
   public byte[] evaluateResponse(byte[] response) throws SaslException {
      return delegate.evaluateResponse(response);
   }

   @Override
   public boolean isComplete() {
      return delegate.isComplete();
   }

   @Override
   public String getAuthorizationID() {
      return delegate.getAuthorizationID();
   }

   @Override
   public byte[] unwrap(byte[] incoming, int offset, int len) throws SaslException {
      return delegate.unwrap(incoming, offset, len);
   }

   @Override
   public byte[] wrap(byte[] outgoing, int offset, int len) throws SaslException {
      return delegate.wrap(outgoing, offset, len);
   }

   @Override
   public Object getNegotiatedProperty(String propName) {
      if (SUBJECT.equals(propName)) {
         if (isComplete()) {
            UserPrincipal userPrincipal = new SimpleUserPrincipal(normalizeAuthorizationId(getAuthorizationID()));
            List<Principal> combinedPrincipals = new ArrayList<>(this.principals.size() + 1);
            combinedPrincipals.add(userPrincipal);
            combinedPrincipals.addAll(this.principals);
            return callbackHandler.getSubjectUserInfo(combinedPrincipals).getSubject();
         } else {
            throw new IllegalStateException("Authentication is not complete");
         }
      } else {
         return delegate.getNegotiatedProperty(propName);
      }
   }

   @Override
   public void dispose() throws SaslException {
      delegate.dispose();
   }

   private static String normalizeAuthorizationId(String id) {
      int realm = id.indexOf('@');
      if (realm >= 0) return id.substring(0, realm);
      else return id;
   }
}
