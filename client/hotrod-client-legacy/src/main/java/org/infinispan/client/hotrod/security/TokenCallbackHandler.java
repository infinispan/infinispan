package org.infinispan.client.hotrod.security;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;

import org.wildfly.security.auth.callback.CredentialCallback;
import org.wildfly.security.credential.BearerTokenCredential;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.1
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
