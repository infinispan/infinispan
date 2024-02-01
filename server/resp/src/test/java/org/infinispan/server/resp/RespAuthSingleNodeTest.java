package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.resp.test.RespTestingUtil.HOST;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import javax.security.auth.Subject;
import javax.security.sasl.SaslException;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.server.resp.authentication.RespAuthenticator;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.testng.annotations.Test;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;
import io.netty.channel.Channel;

/**
 * Test single node with authentication enabled.
 *
 * @author Jose Bolina
 * @since 14.0
 */
@Test(groups = "functional", testName = "server.resp.RespAuthSingleNodeTest")
public class RespAuthSingleNodeTest extends RespSingleNodeTest {
   private static final String USERNAME = "default";
   private static final String PASSWORD = "password";

   @Override
   protected RespServerConfigurationBuilder serverConfiguration(int i) {
      RespServerConfigurationBuilder builder = super.serverConfiguration(i);
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

   public void testNoAuthHello() {
      // Tries to only establish the connection without AUTH parameters.
      RedisURI uri = RedisURI.Builder.redis(HOST, server.getPort()).build();
      try (RedisClient noAuthClient = RedisClient.create(uri)) {
         assertThatThrownBy(noAuthClient::connect)
               .isInstanceOf(RedisConnectionException.class)
               .cause()
               .isInstanceOf(RedisCommandExecutionException.class)
               .hasMessage("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time");
      }
   }

   @Override
   protected RedisPubSubCommands<String, String> createPubSubConnection() {
      RedisPubSubCommands<String, String> connection = super.createPubSubConnection();
      connection.auth(USERNAME, PASSWORD);
      return connection;
   }

   public static class FakeRespAuthenticator implements RespAuthenticator {

      @Override
      public CompletionStage<Subject> clientCertAuth(Channel channel) throws SaslException {
         return CompletableFutures.completedNull();
      }

      @Override
      public CompletionStage<Subject> usernamePasswordAuth(String username, char[] password) {
         if (username.equals(USERNAME) && new String(password).equals(PASSWORD)) {
            return CompletableFuture.completedFuture(new Subject());
         }
         return CompletableFutures.completedNull();
      }

      @Override
      public boolean isClientCertAuthEnabled() {
         return false;
      }
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new RespAuthSingleNodeTest(),
            new RespAuthSingleNodeTest().simpleCache(),
      };
   }

   @Override
   RespSingleNodeTest simpleCache() {
      super.simpleCache();
      return this;
   }
}
