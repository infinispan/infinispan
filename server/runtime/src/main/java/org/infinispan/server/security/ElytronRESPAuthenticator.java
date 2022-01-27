package org.infinispan.server.security;

import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.server.configuration.ServerConfiguration;
import org.infinispan.server.resp.Authenticator;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
import org.infinispan.util.concurrent.BlockingManager;
import org.wildfly.security.auth.server.RealmUnavailableException;
import org.wildfly.security.auth.server.SecurityDomain;
import org.wildfly.security.auth.server.SecurityIdentity;
import org.wildfly.security.evidence.PasswordGuessEvidence;

/**
 * @since 14.0
 **/
public class ElytronRESPAuthenticator implements Authenticator {
   private final String name;
   private SecurityDomain securityDomain;
   private BlockingManager blockingManager;

   public ElytronRESPAuthenticator(String name) {
      this.name = name;
   }

   public static void init(RespServerConfiguration configuration, ServerConfiguration serverConfiguration, BlockingManager blockingManager) {
      ElytronRESPAuthenticator authenticator = (ElytronRESPAuthenticator) configuration.authentication().authenticator();
      if (authenticator != null) {
         authenticator.init(serverConfiguration, blockingManager);
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
            securityIdentity.getRoles().forEach(r -> subject.getPrincipals().add(new RolePrincipal(r)));
            return subject;
         } catch (RealmUnavailableException e) {
            throw new RuntimeException(e);
         }
      }, "resp-auth");
   }
}
