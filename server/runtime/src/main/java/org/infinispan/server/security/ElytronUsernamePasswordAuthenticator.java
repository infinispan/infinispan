package org.infinispan.server.security;

import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.security.GroupPrincipal;
import org.infinispan.server.configuration.ServerConfiguration;
import org.infinispan.server.core.security.UsernamePasswordAuthenticator;
import org.infinispan.server.resp.authentication.RespAuthenticator;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
import org.infinispan.util.concurrent.BlockingManager;
import org.wildfly.security.auth.server.RealmUnavailableException;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityIdentity;
import org.wildfly.security.evidence.PasswordGuessEvidence;

/**
 * @since 14.0
 **/
public class ElytronUsernamePasswordAuthenticator implements UsernamePasswordAuthenticator {
   private final String name;
   private SecurityDomain securityDomain;
   private BlockingManager blockingManager;

   public ElytronUsernamePasswordAuthenticator(String name) {
      this.name = name;
   }

   public static void init(UsernamePasswordAuthenticator authenticator, ServerConfiguration serverConfiguration, BlockingManager blockingManager) {
      if (authenticator instanceof ElytronUsernamePasswordAuthenticator) {
         ((ElytronUsernamePasswordAuthenticator)authenticator).init(serverConfiguration, blockingManager);
      }
   }

   public static void init(RespServerConfiguration configuration, ServerConfiguration serverConfiguration, BlockingManager blockingManager) {
      RespAuthenticator authenticator = configuration.authentication().authenticator();
      if (authenticator != null) {
         ((ElytronRESPAuthenticator) authenticator).init(serverConfiguration, blockingManager);
      }
   }

   private void init(ServerConfiguration serverConfiguration, BlockingManager blockingManager) {
      securityDomain = serverConfiguration.security().realms().getRealm(name).serverSecurityRealm().getSecurityDomain();
      this.blockingManager = blockingManager;
   }

   @Override
   public CompletionStage<Subject> authenticate(String username, char[] password) {
      return blockingManager.supplyBlocking(() -> {
         try {
            SecurityIdentity securityIdentity = securityDomain.authenticate(username, new PasswordGuessEvidence(password));
            Subject subject = new Subject();
            subject.getPrincipals().add(securityIdentity.getPrincipal());
            securityIdentity.getRoles().forEach(r -> subject.getPrincipals().add(new GroupPrincipal(r)));
            return subject;
         } catch (RealmUnavailableException e) {
            throw new RuntimeException(e);
         }
      }, "elytron-auth");
   }
}
