package org.infinispan.server.security.authorization;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.server.test.api.RespTestClientDriver;
import org.infinispan.server.test.api.TestUser;
import org.infinispan.server.test.junit5.InfinispanServerExtension;
import org.junit.jupiter.api.Test;

import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.sync.BaseRedisCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.api.sync.RedisServerCommands;
import io.lettuce.core.cluster.api.sync.RedisClusterCommands;
import io.lettuce.core.resource.DefaultClientResources;

abstract class RESPAuthorizationTest {

   protected final InfinispanServerExtension ext;

   protected final boolean cert;
   protected final Function<TestUser, String> serverPrincipal;
   protected final Map<TestUser, RespTestClientDriver.LettuceConfiguration> respBuilders;

   public RESPAuthorizationTest(InfinispanServerExtension ext) {
      this(ext, false, TestUser::getUser, user -> {
         InetSocketAddress serverSocket = ext.getServerDriver().getServerSocket(0, 11222);
         ClientOptions clientOptions = ClientOptions.builder()
               .autoReconnect(false)
               .build();
         RedisURI.Builder uriBuilder = RedisURI.builder()
               .withHost(serverSocket.getHostString())
               .withPort(serverSocket.getPort());

         if (user != TestUser.ANONYMOUS) {
            uriBuilder = uriBuilder.withAuthentication(user.getUser(), user.getPassword());
         }
         return new RespTestClientDriver.LettuceConfiguration(DefaultClientResources.builder(), clientOptions, uriBuilder.build());
      });
   }

   public RESPAuthorizationTest(InfinispanServerExtension ext, boolean cert, Function<TestUser, String> serverPrincipal, Function<TestUser, RespTestClientDriver.LettuceConfiguration> respBuilder) {
      this.ext = ext;
      this.cert = cert;
      this.serverPrincipal = serverPrincipal;
      this.respBuilders = Stream.of(TestUser.values()).collect(Collectors.toMap(user -> user, respBuilder));
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
            RedisCommands<String, String> conn = createConnection(client);
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

            RedisCommands<String, String> conn = createConnection(client);
            assertThat(conn.info()).isNotEmpty();
         }
      }
   }

   @Test
   public void testClusterSHARDS() {
      for (TestUser user: TestUser.values()) {
         try (RedisClient client = createClient(user)) {
            if (user == TestUser.ANONYMOUS) {
               assertAnonymous(client, RedisServerCommands::info);
               continue;
            }

            RedisCommands<String, String> conn = createConnection(client);
            List<Object> shards = conn.clusterShards();
            assertThat(shards)
                  .isNotEmpty()
                  .size().isEqualTo(2);
         }
      }
   }

   @Test
   public void testHMSETCommand() {
      RedisCommands<String, String> redis = createConnection(TestUser.ADMIN);

      Map<String, String> map = Map.of("key1", "value1", "key2", "value2", "key3", "value3");
      assertThat(redis.hmset("HMSET", map)).isEqualTo("OK");

      assertThat(redis.hget("HMSET", "key1")).isEqualTo("value1");
      assertThat(redis.hget("HMSET", "unknown")).isNull();

      assertThat(redis.set("plain", "string")).isEqualTo("OK");
      assertThatThrownBy(() -> redis.hmset("plain", Map.of("k1", "v1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERRWRONGTYPE");
   }

   @Test
   public void testClusterNodes() {
      for (TestUser user: TestUser.values()) {
         try (RedisClient client = createClient(user)) {
            if (user == TestUser.ANONYMOUS) {
               assertAnonymous(client, RedisClusterCommands::clusterNodes);
               continue;
            }

            RedisCommands<String, String> conn = createConnection(client);
            String nodes = conn.clusterNodes();
            assertThat(nodes).isNotNull().isNotEmpty();
            assertThat(nodes.split("\n"))
                  .isNotEmpty()
                  .hasSize(2);
         }
      }
   }

   private void assertAnonymous(RedisClient client, Consumer<RedisCommands<String, String>> consumer) {
      // When using certificates, an anonymous user can not connect.
      if (cert) {
         assertThatThrownBy(client::connect).isExactlyInstanceOf(RedisConnectionException.class);
         return;
      }

      // If using USERNAME + PASSWORD, redis accepts anonymous connections.
      // But following requests will fail, as user needs to be authenticated.
      RedisCommands<String, String> conn = createConnection(client);
      assertThatThrownBy(() -> consumer.accept(conn)).isExactlyInstanceOf(RedisCommandExecutionException.class);
   }

   private RedisClient createClient(TestUser user) {
      RespTestClientDriver.LettuceConfiguration config = respBuilders.get(user);

      if (config == null) {
         fail(this.getClass().getSimpleName() + " does not define configuration for user " + user);
      }

      return ext.resp().withConfiguration(config).get();
   }

   private RedisCommands<String, String> createConnection(TestUser user) {
      RespTestClientDriver.LettuceConfiguration config = respBuilders.get(user);

      if (config == null) {
         fail(this.getClass().getSimpleName() + " does not define configuration for user " + user);
      }

      return ext.resp()
            .withConfiguration(config)
            .getConnection()
            .sync();
   }

   private RedisCommands<String, String> createConnection(RedisClient client) {
      return ext.resp()
            .connect(client)
            .sync();
   }
}
