package org.infinispan.server.security;

import java.io.IOException;
import java.util.EnumSet;
import java.util.function.BiPredicate;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.infinispan.security.AuthorizationPermission;
import org.infinispan.security.GroupPrincipal;
import org.infinispan.security.impl.Authorizer;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.ServerConfiguration;
import org.infinispan.server.loader.ServerLoginModule;
import org.infinispan.server.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityIdentity;
import org.wildfly.security.evidence.PasswordGuessEvidence;

/**
 * @since 14.0
 **/
public class ElytronJMXAuthenticator implements BiPredicate<CallbackHandler, Subject> {
   private static final Log log = LogFactory.getLog(ElytronJMXAuthenticator.class, Log.class);
   private final SecurityDomain securityDomain;
   private final Authorizer authorizer;

   private ElytronJMXAuthenticator(Authorizer authorizer, SecurityDomain securityDomain) {
      this.authorizer = authorizer;
      this.securityDomain = securityDomain;
   }

   public static void init(Authorizer authorizer, ServerConfiguration serverConfiguration) {
      BiPredicate<CallbackHandler, Subject> authenticator;
      String securityRealm = serverConfiguration.endpoints().securityRealm();
      if (securityRealm != null) {
         SecurityDomain securityDomain = serverConfiguration.security().realms().realms().get(securityRealm).serverSecurityRealm().getSecurityDomain();
         authenticator = new ElytronJMXAuthenticator(authorizer, securityDomain);
         Server.log.debugf("Initializing JMX authenticator for realm %s", securityRealm);
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
         if (log.isTraceEnabled()) {
            log.trace("Before JMX callbackhandler");
         }
         callbackHandler.handle(new Callback[]{name, password});
         if (log.isTraceEnabled()) {
            log.tracef("Obtained %s username from JMX callbackhandler. Authenticating with realm", name.getName());
         }
         SecurityIdentity securityIdentity = securityDomain.authenticate(name.getName(), new PasswordGuessEvidence(password.getPassword()));
         subject.getPrincipals().add(securityIdentity.getPrincipal());
         securityIdentity.getRoles().forEach(r -> subject.getPrincipals().add(new GroupPrincipal(r)));
         EnumSet<AuthorizationPermission> permissions = authorizer.getPermissions(null, subject);
         if (permissions.contains(AuthorizationPermission.ADMIN)) {
            subject.getPrincipals().add(new GroupPrincipal("controlRole"));
         }
         if (permissions.contains(AuthorizationPermission.MONITOR)) {
            subject.getPrincipals().add(new GroupPrincipal("monitorRole"));
         }
         if (log.isTraceEnabled()) {
            log.tracef("Successfully authenticated %s", subject);
         }
         return true;
      } catch (IOException | UnsupportedCallbackException e) {
         Server.log.jmxAuthenticationError(e);
         return false;
      }
   }
}
