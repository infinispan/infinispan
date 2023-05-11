package org.infinispan.server.security;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.ScheduledExecutorService;

import javax.security.auth.Subject;
import javax.security.sasl.SaslException;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.ServerConfiguration;
import org.infinispan.server.core.security.UsernamePasswordAuthenticator;
import org.infinispan.server.resp.authentication.RespAuthenticator;
import org.infinispan.util.concurrent.BlockingManager;

import io.netty.channel.Channel;

public class ElytronRESPAuthenticator implements RespAuthenticator {

   private UsernamePasswordAuthenticator usernamePasswordAuthenticator;
   private RespClientCertAuthenticator clientCertAuthenticator;


   @Override
   public CompletionStage<Subject> clientCertAuth(Channel channel) throws SaslException {
      if (clientCertAuthenticator == null) {
         Server.log.debug("No usernamePasswordAuthenticator configured for RESP");
         return CompletableFutures.completedNull();
      }
      return clientCertAuthenticator.clientCertAuth(channel);
   }

   @Override
   public CompletionStage<Subject> usernamePasswordAuth(String username, char[] password) {
      if (usernamePasswordAuthenticator == null) {
         Server.log.debug("No usernamePasswordAuthenticator configured for RESP");
         return CompletableFutures.completedNull();
      }
      return usernamePasswordAuthenticator.authenticate(username, password);
   }

   @Override
   public boolean isClientCertAuthEnabled() {
      return clientCertAuthenticator != null;
   }

   public void withUsernamePasswordAuth(UsernamePasswordAuthenticator usernamePasswordAuthenticator) {
      this.usernamePasswordAuthenticator = usernamePasswordAuthenticator;
   }

   public void withClientCertAuth(RespClientCertAuthenticator clientCertAuthenticator) {
      this.clientCertAuthenticator = clientCertAuthenticator;
   }

   void init(ServerConfiguration configuration, BlockingManager blockingManager) {
      if (usernamePasswordAuthenticator != null) {
         ElytronUsernamePasswordAuthenticator.init(usernamePasswordAuthenticator, configuration, blockingManager);
      }
   }

   void init(ServerConfiguration configuration, ScheduledExecutorService timeoutExecutor) {
      if (clientCertAuthenticator != null) {
         clientCertAuthenticator.init(configuration, timeoutExecutor);
      }
   }
}
