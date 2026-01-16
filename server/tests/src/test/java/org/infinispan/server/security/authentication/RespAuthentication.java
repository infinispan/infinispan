package org.infinispan.server.security.authentication;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.security.AuthorizationPermission;
import org.infinispan.server.test.api.TestClientDriver;
import org.infinispan.server.test.core.tags.Security;
import org.infinispan.server.test.junit5.InfinispanServer;
import org.infinispan.testing.Exceptions;
import org.junit.jupiter.api.Test;

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
@Security
public class RespAuthentication {

   @InfinispanServer(AuthenticationIT.class)
   public static TestClientDriver SERVERS;

   @Test
   public void testRestReadWrite() {
      RedisClient client = RedisClient.create(SERVERS.resp().withUser(AuthorizationPermission.ALL)
            .connectionString(0));
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
      RedisClient client = RedisClient.create(SERVERS.resp().withUser(AuthorizationPermission.ALL)
            .connectionString(0));
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
