package org.infinispan.server.security.authentication;

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.test.Exceptions;
import org.infinispan.server.test.core.category.Security;
import org.infinispan.server.test.junit4.InfinispanServerRule;
import org.infinispan.server.test.junit4.InfinispanServerTestMethodRule;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.ListOfMapsOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;

/**
 * @since 14.0
 **/
@Category(Security.class)
public class RespAuthentication {

   @ClassRule
   public static InfinispanServerRule SERVERS = AuthenticationIT.SERVERS;

   @Rule
   public InfinispanServerTestMethodRule SERVER_TEST = new InfinispanServerTestMethodRule(SERVERS);

   @Test
   public void testRestReadWrite() {
      InetSocketAddress serverSocket = SERVERS.getServerDriver().getServerSocket(0, 11222);
      RedisClient client = RedisClient.create(String.format("redis://all_user:all@%s:%d", serverSocket.getHostName(), serverSocket.getPort()));
      try (StatefulRedisConnection<String, String> redisConnection = client.connect()) {
         RedisCommands<String, String> redis = redisConnection.sync();
         redis.set("k1", "v1");
         redis.set("k3", "v3");
         redis.set("k4", "v4");

         List<KeyValue<String, String>> expected = new ArrayList<>(4);
         expected.add(KeyValue.just("k1", "v1"));
         expected.add(KeyValue.empty("k2"));
         expected.add(KeyValue.just("k3", "v3"));
         expected.add(KeyValue.just("k4", "v4"));

         List<KeyValue<String, String>> results = redis.mget("k1", "k2", "k3", "k4");
         assertEquals(expected, results);
      } finally {
         client.shutdown();
      }
   }

   @Test
   public void testRespCommandDocs() {
      InetSocketAddress serverSocket = SERVERS.getServerDriver().getServerSocket(0, 11222);
      RedisClient client = RedisClient.create(String.format("redis://all_user:all@%s:%d", serverSocket.getHostName(), serverSocket.getPort()));
      try (StatefulRedisConnection<String, String> redisConnection = client.connect()) {
         RedisCommands<String, String> redis = redisConnection.sync();
         Exceptions.expectException(RedisCommandExecutionException.class, () -> redis.dispatch(CommandType.COMMAND,
               new ListOfMapsOutput<>(StringCodec.UTF8),
               new CommandArgs<>(StringCodec.UTF8).add("DOCS")));
      } finally {
         client.shutdown();
      }
   }
}
