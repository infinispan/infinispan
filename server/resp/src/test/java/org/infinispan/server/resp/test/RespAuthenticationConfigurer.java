package org.infinispan.server.resp.test;

import static org.infinispan.server.resp.test.RespTestingUtil.ADMIN;
import static org.infinispan.server.resp.test.RespTestingUtil.HOST;
import static org.infinispan.server.resp.test.RespTestingUtil.PASSWORD;
import static org.infinispan.server.resp.test.RespTestingUtil.USERNAME;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;
import javax.security.sasl.SaslException;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.resp.authentication.RespAuthenticator;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.netty.channel.Channel;

public final class RespAuthenticationConfigurer {

   public static RespServerConfigurationBuilder enableAuthentication(RespServerConfigurationBuilder builder) {
      builder.authentication()
            .enable()
            .authenticator(new FakeRespAuthenticator());
      return builder;
   }

   public static RedisClient createAuthenticationClient(int port) {
      RedisURI uri = RedisURI.Builder
            .redis(HOST, port)
            .withAuthentication(USERNAME, PASSWORD)
            .withTimeout(Duration.ofMillis(15_000))
            .build();
      return RedisClient.create(uri);
   }

   private static final class FakeRespAuthenticator implements RespAuthenticator {
      @Override
      public CompletionStage<Subject> clientCertAuth(Channel channel) throws SaslException {
         return CompletableFutures.completedNull();
      }

      @Override
      public CompletionStage<Subject> usernamePasswordAuth(String username, char[] password) {
         if (username.equals(USERNAME) && new String(password).equals(PASSWORD)) {
            return CompletableFuture.completedFuture(ADMIN);
         }
         return CompletableFutures.completedNull();
      }

      @Override
      public boolean isClientCertAuthEnabled() {
         return false;
      }
   }
}
