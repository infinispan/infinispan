package org.infinispan.server.test.util.security;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.RealmCallback;

/**
 * 
 * SimpleLoginHandler which pass login and password for authorization.
 * 
 * @author vjuranek
 * @since 7.0
 */
public class SimpleLoginHandler implements CallbackHandler {
   final private String login;
   final private String password;
   final private String realm;

   public SimpleLoginHandler(String login, String password) {
      this.login = login;
      this.password = password;
      this.realm = null;
   }

   public SimpleLoginHandler(String login, String password, String realm) {
      this.login = login;
      this.password = password;
      this.realm = realm;
   }

   @Override
   public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      for (Callback callback : callbacks) {
         if (callback instanceof NameCallback) {
            ((NameCallback) callback).setName(login);
         } else if (callback instanceof PasswordCallback) {
            ((PasswordCallback) callback).setPassword(password.toCharArray());
         } else if (callback instanceof RealmCallback) {
            ((RealmCallback) callback).setText(realm);
         } else {
            throw new UnsupportedCallbackException(callback);
         }
      }
   }
}