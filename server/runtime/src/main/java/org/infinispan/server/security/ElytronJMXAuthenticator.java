package org.infinispan.server.security;

import java.io.IOException;
import java.util.function.BiPredicate;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.infinispan.server.Server;
import org.infinispan.server.configuration.ServerConfiguration;
import org.infinispan.server.loader.ServerLoginModule;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityIdentity;
import org.wildfly.security.evidence.PasswordGuessEvidence;

/**
 * @since 14.0
 **/
public class ElytronJMXAuthenticator implements BiPredicate<CallbackHandler, Subject> {
   private final SecurityDomain securityDomain;

   private ElytronJMXAuthenticator(SecurityDomain securityDomain) {
      this.securityDomain = securityDomain;
   }

   public static void init(ServerConfiguration serverConfiguration) {
      BiPredicate<CallbackHandler, Subject> authenticator;
      String securityRealm = serverConfiguration.endpoints().securityRealm();
      if (securityRealm != null) {
         SecurityDomain securityDomain = serverConfiguration.security().realms().realms().get(securityRealm).serverSecurityRealm().getSecurityDomain();
         authenticator = new ElytronJMXAuthenticator(securityDomain);
      } else {
         Server.log.jmxNoDefaultSecurityRealm();
         authenticator = (c, s) -> false;
      }
      ServerLoginModule.setAuthenticator(authenticator);
   }

   @Override
   public boolean test(CallbackHandler callbackHandler, Subject subject) {
      NameCallback name = new NameCallback("username");
      PasswordCallback password = new PasswordCallback("password", false);
      try {
         callbackHandler.handle(new Callback[]{name, password});
         SecurityIdentity securityIdentity = securityDomain.authenticate(name.getName(), new PasswordGuessEvidence(password.getPassword()));
         subject.getPrincipals().add(securityIdentity.getPrincipal());
         securityIdentity.getRoles().forEach(r -> subject.getPrincipals().add(new RolePrincipal(r)));
         return true;
      } catch (IOException | UnsupportedCallbackException e) {
         Server.log.jmxAuthenticationError(e);
         return false;
      }
   }
}
