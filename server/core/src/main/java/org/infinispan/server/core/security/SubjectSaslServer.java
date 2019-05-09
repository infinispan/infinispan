package org.infinispan.server.core.security;

import java.security.Principal;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

/**
 * A {@link SaslServer} which, when complete, can return a negotiated property named {@link #SUBJECT} which contains a
 * populated {@link Subject} representing the authenticated user.
 *
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public class SubjectSaslServer implements SaslServer {
   public static final String SUBJECT = "org.infinispan.security.Subject";
   final SaslServer delegate;
   private final AuthorizingCallbackHandler callbackHandler;
   private final Set<Principal> principals;

   public SubjectSaslServer(SaslServer delegate, Set<Principal> principals, AuthorizingCallbackHandler callbackHandler) {
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
            return callbackHandler.getSubjectUserInfo(principals).getSubject();
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
}
