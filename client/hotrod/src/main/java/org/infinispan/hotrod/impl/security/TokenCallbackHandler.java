package org.infinispan.hotrod.impl.security;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;

import org.wildfly.security.auth.callback.CredentialCallback;
import org.wildfly.security.credential.BearerTokenCredential;

/**
 * @since 14.0
 **/
public class TokenCallbackHandler implements CallbackHandler {
   private volatile String token;

   public TokenCallbackHandler(String token) {
      this.token = token;
   }

   @Override
   public void handle(Callback[] callbacks) {
      for (Callback callback : callbacks) {
         if (callback instanceof CredentialCallback) {
            CredentialCallback cc = (CredentialCallback) callback;
            cc.setCredential(new BearerTokenCredential(token));
         }
      }
   }

   public String getToken() {
      return token;
   }

   public void setToken(String token) {
      this.token = token;
   }
}
