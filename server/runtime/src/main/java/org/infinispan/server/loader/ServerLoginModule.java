package org.infinispan.server.loader;

import java.util.Map;
import java.util.function.BiPredicate;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.spi.LoginModule;

/**
 * @since 14.0
 **/
public class ServerLoginModule implements LoginModule {
   private Subject subject;
   private CallbackHandler callbackHandler;
   private static BiPredicate<CallbackHandler, Subject> AUTHENTICATOR;

   public static void setAuthenticator(BiPredicate<CallbackHandler, Subject> authenticator) {
      AUTHENTICATOR = authenticator;
   }

   @Override
   public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
      this.subject = subject;
      this.callbackHandler = callbackHandler;
   }

   @Override
   public boolean login() {
      return (AUTHENTICATOR != null && AUTHENTICATOR.test(callbackHandler, subject));
   }

   @Override
   public boolean commit() {
      return true;
   }

   @Override
   public boolean abort() {
      return true;
   }

   @Override
   public boolean logout() {
      return true;
   }
}
