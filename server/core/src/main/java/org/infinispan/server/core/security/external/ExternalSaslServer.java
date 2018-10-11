package org.infinispan.server.core.security.external;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.security.Principal;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.infinispan.commons.util.Util;

final class ExternalSaslServer implements SaslServer {
   private final AtomicBoolean complete = new AtomicBoolean();
   private String authorizationID;
   private final Principal peerPrincipal;
   private final CallbackHandler callbackHandler;

   ExternalSaslServer(final CallbackHandler callbackHandler, final Principal peerPrincipal) {
      this.callbackHandler = callbackHandler;
      this.peerPrincipal = peerPrincipal;
   }

   public String getMechanismName() {
      return "EXTERNAL";
   }

   public byte[] evaluateResponse(final byte[] response) throws SaslException {
      if (complete.getAndSet(true)) {
         throw new SaslException("Received response after complete");
      }
      String userName;
      try {
         userName = new String(response, "UTF-8");
      } catch (UnsupportedEncodingException e) {
         throw new SaslException("Cannot convert user name from UTF-8", e);
      }
      if (userName.length() == 0) {
         userName = peerPrincipal.getName();
      }
      final AuthorizeCallback authorizeCallback = new AuthorizeCallback(peerPrincipal.getName(), userName);
      handleCallback(callbackHandler, authorizeCallback);
      if (authorizeCallback.isAuthorized()) {
         authorizationID = authorizeCallback.getAuthorizedID();
      } else {
         throw new SaslException("EXTERNAL: " + peerPrincipal.getName() + " is not authorized to act as " + userName);
      }

      return Util.EMPTY_BYTE_ARRAY;
   }

   private static void handleCallback(CallbackHandler handler, Callback callback) throws SaslException {
      try {
         handler.handle(new Callback[]{
               callback,
         });
      } catch (SaslException e) {
         throw e;
      } catch (IOException e) {
         throw new SaslException("Failed to authenticate due to callback exception", e);
      } catch (UnsupportedCallbackException e) {
         throw new SaslException("Failed to authenticate due to unsupported callback", e);
      }
   }

   public boolean isComplete() {
      return complete.get();
   }

   public String getAuthorizationID() {
      return authorizationID;
   }

   public byte[] unwrap(final byte[] incoming, final int offset, final int len) throws SaslException {
      throw new IllegalStateException();
   }

   public byte[] wrap(final byte[] outgoing, final int offset, final int len) throws SaslException {
      throw new IllegalStateException();
   }

   public Object getNegotiatedProperty(final String propName) {
      return null;
   }

   public void dispose() throws SaslException {
   }
}
