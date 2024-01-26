package org.infinispan.server.resp;

import static org.infinispan.server.resp.test.RespTestingUtil.HOST;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;

import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;

/**
 * Test single node with authentication enabled.
 *
 * @author Jose Bolina
 * @since 14.0
 */
@CleanupAfterMethod
@Test(groups = "functional", testName = "server.resp.RespAuthSingleNodeTest")
public class RespAuthSingleNodeTest extends RespSingleNodeTest {
   private static final String USERNAME = "admin";
   private static final String PASSWORD = "super-password";

   @Override
   protected RespServerConfigurationBuilder serverConfiguration() {
      RespServerConfigurationBuilder builder = super.serverConfiguration();
      builder.authentication()
            .enable()
            .authenticator(new FakeRespAuthenticator());
      return builder;
   }

   @Override
   protected RedisClient createRedisClient(int port) {
      RedisURI uri = RedisURI.Builder
            .redis(HOST, port)
            .withAuthentication(USERNAME, PASSWORD)
            .withTimeout(Duration.ofMillis(timeout))
            .build();
      return RedisClient.create(uri);
   }

   @Override
   protected void teardown() {
      super.destroyAfterClass();
   }

   public void testNoAuthHello() {
      // Tries to only establish the connection without AUTH parameters.
      RedisURI uri = RedisURI.Builder.redis(HOST, server.getPort()).build();
      try (RedisClient noAuthClient = RedisClient.create(uri)) {
         Exceptions.expectException(RedisConnectionException.class,
               RedisCommandExecutionException.class,
               "NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time",
               noAuthClient::connect);
      }
   }

   @Override
   protected RedisPubSubCommands<String, String> createPubSubConnection() {
      RedisPubSubCommands<String, String> connection = super.createPubSubConnection();
      connection.auth(USERNAME, PASSWORD);
      return connection;
   }

   public static class FakeRespAuthenticator implements Authenticator {
      @Override
      public CompletionStage<Subject> authenticate(String username, char[] password) {
         if (username.equals(USERNAME) && new String(password).equals(PASSWORD)) {
            return CompletableFuture.completedFuture(new Subject());
         }
         return CompletableFutures.completedNull();
      }
   }
}
