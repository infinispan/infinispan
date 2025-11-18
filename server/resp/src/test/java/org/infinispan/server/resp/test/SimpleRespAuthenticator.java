package org.infinispan.server.resp.test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;
import javax.security.sasl.SaslException;

import org.infinispan.server.core.security.simple.SimpleUserPrincipal;
import org.infinispan.server.resp.authentication.RespAuthenticator;

import io.netty.channel.Channel;

public class SimpleRespAuthenticator implements RespAuthenticator {
   private final Map<String, String> users = new HashMap<>();

   public SimpleRespAuthenticator() {}

   public void addUser(String username, String password) {
      users.put(username, password);
   }

   @Override
   public CompletionStage<Subject> clientCertAuth(Channel channel) throws SaslException {
      throw new UnsupportedOperationException();
   }

   @Override
   public CompletionStage<Subject> usernamePasswordAuth(String username, char[] password) {
      String secret = users.get(username);
      if (secret == null) {
         throw new IllegalStateException("Unknown user");
      } else if (!secret.equals(new String(password))) {
         throw new IllegalStateException("Wrong secret");
      } else {
         Subject subject = new Subject();
         subject.getPrincipals().add(new SimpleUserPrincipal(username));
         return CompletableFuture.completedStage(subject);
      }
   }

   @Override
   public boolean isClientCertAuthEnabled() {
      return false;
   }
}
