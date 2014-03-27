package org.infinispan.test.integration.security.utils;

import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

/** 
 * @author vjuranek
 * @since 7.0
 */
public class LoginHandler implements CallbackHandler {

   private final String login;
   private final String password;

   public LoginHandler(String login, String password) {
      this.login = login;
      this.password = password;
   }

   public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      for (Callback callback : callbacks) {
         if (callback instanceof NameCallback) {
            ((NameCallback) callback).setName(login);
         }
         if (callback instanceof PasswordCallback) {
            ((PasswordCallback) callback).setPassword(password.toCharArray());
         }
      }
   }

}