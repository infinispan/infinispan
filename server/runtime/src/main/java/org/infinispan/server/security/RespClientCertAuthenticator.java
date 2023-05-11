package org.infinispan.server.security;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;

import javax.security.auth.Subject;
import javax.security.sasl.SaslException;
import javax.security.sasl.SaslServer;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.configuration.ServerConfiguration;
import org.infinispan.server.core.security.sasl.SaslAuthenticator;
import org.infinispan.server.core.security.sasl.SubjectSaslServer;
import org.wildfly.security.sasl.util.SaslMechanismInformation;

import io.netty.channel.Channel;

public class RespClientCertAuthenticator {

   private final String realmName;
   private final ElytronSASLAuthenticator authenticator;

   public RespClientCertAuthenticator(String realmName) {
      this.realmName = realmName;
      authenticator = new ElytronSASLAuthenticator(realmName, null, Collections.singleton(SaslMechanismInformation.Names.EXTERNAL));
   }


   public CompletionStage<Subject> clientCertAuth(Channel channel) throws SaslException {
      SaslServer server = null;
      try {
         server = SaslAuthenticator.createSaslServer(authenticator, null, channel, SaslMechanismInformation.Names.EXTERNAL, "resp");
         if (server == null) {
            return CompletableFutures.completedNull();
         }

         server.evaluateResponse(new byte[0]);
         return CompletableFuture.completedFuture((Subject) server.getNegotiatedProperty(SubjectSaslServer.SUBJECT));
      } catch (Throwable e) {
         throw new RuntimeException(e);
      } finally {
         if (server != null) {
            server.dispose();
         }
      }
   }

   public void init(ServerConfiguration configuration, ScheduledExecutorService timeoutExecutor) {
      authenticator.init(configuration, timeoutExecutor);
   }
}
