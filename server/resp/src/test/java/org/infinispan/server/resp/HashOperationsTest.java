package org.infinispan.server.resp;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.util.Map;

import org.testng.annotations.Test;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "server.resp.HashOperationsTest")
public class HashOperationsTest extends SingleNodeRespBaseTest {

   public void testHMSET() {
      RedisCommands<String, String> redis = redisConnection.sync();

      Map<String, String> map = Map.of("key1", "value1", "key2", "value2", "key3", "value3");
      assertThat(redis.hmset("HMSET", map)).isEqualTo("OK");

      // TODO: check when get method added.

      assertThat(redis.set("plain", "string")).isEqualTo("OK");
      assertThatThrownBy(() -> redis.hmset("plain", Map.of("k1", "v1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERRWRONGTYPE");
   }

   public void testHSET() {
      RedisCommands<String, String> redis = redisConnection.sync();

      Map<String, String> map = Map.of("key1", "value1", "key2", "value2", "key3", "value3");
      assertThat(redis.hset("HSET", map)).isEqualTo(3);

      // Updating some keys should return 0.
      assertThat(redis.hset("HSET", Map.of("key1", "other-value1"))).isEqualTo(0);

      // Mixing update and new keys.
      assertThat(redis.hset("HSET", Map.of("key2", "other-value2", "key4", "value4"))).isEqualTo(1);

      // TODO: check when get method added.

      assertThat(redis.set("plain", "string")).isEqualTo("OK");
      assertThatThrownBy(() -> redis.hmset("plain", Map.of("k1", "v1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERRWRONGTYPE");
   }
}
