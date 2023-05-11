package org.infinispan.server.security.authorization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.junit.AssumptionViolatedException;
import org.junit.Test;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;

public abstract class RespAbstractAuthorization {
   final Map<TestUser, LettuceConfiguration> respBuilders;

   protected RespAbstractAuthorization() {
      this.respBuilders = new HashMap<>();
   }

   protected void addClientBuilders(TestUser user) {
      InetSocketAddress serverSocket = getServers().getServerDriver().getServerSocket(0, 11222);
      DefaultClientResources resources = DefaultClientResources.builder().build();
      ClientOptions clientOptions = ClientOptions.builder()
            .autoReconnect(false)
            .build();
      RedisURI.Builder uriBuilder = RedisURI.builder()
            .withHost(serverSocket.getHostString())
            .withPort(serverSocket.getPort());

      if (user != TestUser.ANONYMOUS) {
         uriBuilder = uriBuilder.withAuthentication(user.getUser(), user.getPassword());
      }
      respBuilders.put(user, new LettuceConfiguration(resources, clientOptions, uriBuilder.build()));
   }

   protected abstract InfinispanServerRule getServers();

   protected static class LettuceConfiguration {
      private final ClientResources clientResources;
      private final ClientOptions clientOptions;
      private final RedisURI redisURI;

      protected LettuceConfiguration(ClientResources clientResources, ClientOptions clientOptions, RedisURI redisURI) {
         this.clientResources = clientResources;
         this.clientOptions = clientOptions;
         this.redisURI = redisURI;
      }
   }

   protected boolean isUsingCert() {
      return false;
   }

   @Test
   public void testSetGetDelete() {
      RedisCommands<String, String> redis = createConnection(TestUser.ADMIN);

      redis.set("k1", "v1");
      String v = redis.get("k1");
      assertThat(v).isEqualTo("v1");

      redis.del("k1");

      assertThat(redis.get("k1")).isNull();
      assertThat(redis.get("something")).isNull();
   }

   @Test
   public void testAllUsersConnectingAndOperating() {
      for (TestUser user: TestUser.values()) {
         try (RedisClient client = createClient(user)) {
            if (user == TestUser.ANONYMOUS) {
               assertAnonymous(client, BaseRedisCommands::ping);
               continue;
            }
            RedisCommands<String, String> conn = client.connect().sync();
            assertThat(conn.ping()).isEqualTo("PONG");
         }
      }
   }

   @Test
   public void testRPUSH() {
      RedisCommands<String, String> redis = createConnection(TestUser.ADMIN);
      long result = redis.rpush("people", "tristan");
      assertThat(result).isEqualTo(1);

      result = redis.rpush("people", "william");
      assertThat(result).isEqualTo(2);

      result = redis.rpush("people", "william", "jose", "pedro");
      assertThat(result).isEqualTo(5);

      // Set a String Command
      redis.set("leads", "tristan");

      // Push on an existing key that contains a String, not a Collection!
      assertThatThrownBy(() -> {
         redis.rpush("leads", "william");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERRWRONGTYPE");
   }

   @Test
   public void testInfoCommand() {
      for (TestUser user: TestUser.values()) {

         try (RedisClient client = createClient(user)) {
            if (user == TestUser.ANONYMOUS) {
               assertAnonymous(client, RedisServerCommands::info);
               continue;
            }

            RedisCommands<String, String> conn = client.connect().sync();
            assertThat(conn.info()).isNotEmpty();
         }
      }
   }

   private void assertAnonymous(RedisClient client, Consumer<RedisCommands<String, String>> consumer) {
      // When using certificates, an anonymous user can not connect.
      if (isUsingCert()) {
         assertThatThrownBy(client::connect).isExactlyInstanceOf(RedisConnectionException.class);
         return;
      }

      // If using USERNAME + PASSWORD, redis accepts anonymous connections.
      // But following requests will fail, as user needs to be authenticated.
      RedisCommands<String, String> conn = client.connect().sync();
      assertThatThrownBy(() -> consumer.accept(conn)).isExactlyInstanceOf(RedisCommandExecutionException.class);
   }

   private RedisClient createClient(TestUser user) {
      LettuceConfiguration config = respBuilders.get(user);

      if (config == null) {
         throw new AssumptionViolatedException(this.getClass().getSimpleName() + " does not define configuration for user " + user);
      }

      RedisClient client = RedisClient.create(config.clientResources, config.redisURI);
      client.setOptions(config.clientOptions);
      return client;
   }

   private RedisCommands<String, String> createConnection(TestUser user) {
      RedisClient redisClient = createClient(user);
      LettuceConfiguration configuration = respBuilders.get(user);
      redisClient.setOptions(configuration.clientOptions);

      StatefulRedisConnection<String, String> connection = redisClient.connect();
      return connection.sync();
   }
}
