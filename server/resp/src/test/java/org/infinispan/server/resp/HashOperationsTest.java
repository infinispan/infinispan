package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

import io.lettuce.core.MapScanCursor;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "server.resp.HashOperationsTest")
public class HashOperationsTest extends SingleNodeRespBaseTest {

   public void testHMSET() {
      RedisCommands<String, String> redis = redisConnection.sync();

      Map<String, String> map = Map.of("key1", "value1", "key2", "value2", "key3", "value3");
      assertThat(redis.hmset("HMSET", map)).isEqualTo("OK");

      assertThat(redis.hget("HMSET", "key1")).isEqualTo("value1");
      assertThat(redis.hget("HMSET", "unknown")).isNull();
      assertThat(redis.hget("UNKNOWN", "unknown")).isNull();

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

      assertThat(redis.hget("HSET", "key1")).isEqualTo("other-value1");
      assertThat(redis.hget("HSET", "unknown")).isNull();
      assertThat(redis.hget("UNKNOWN", "unknown")).isNull();

      assertThat(redis.set("plain", "string")).isEqualTo("OK");
      assertThatThrownBy(() -> redis.hmset("plain", Map.of("k1", "v1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERRWRONGTYPE");
      assertThatThrownBy(() -> redis.hget("plain", "k1"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERRWRONGTYPE");
   }

   public void testHashLength() {
      RedisCommands<String, String> redis = redisConnection.sync();

      Map<String, String> map = Map.of("key1", "value1", "key2", "value2", "key3", "value3");
      assertThat(redis.hset("len-test", map)).isEqualTo(3);

      assertThat(redis.hlen("len-test")).isEqualTo(3);
      assertThat(redis.hlen("UNKNOWN")).isEqualTo(0);

      redis.set("plain", "string");
      assertThatThrownBy(() -> redis.hlen("plain"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERRWRONGTYPE");
   }

   public void testHScanOperation() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Map<String, String> content = new HashMap<>();

      int dataSize = 15;
      redis.flushdb();
      for (int i = 0; i < dataSize; i++) {
         content.put("key" + i, "value" + i);
      }

      assertThat(redis.hmset("hscan-test", content)).isEqualTo("OK");
      assertThat(redis.hlen("hscan-test")).isEqualTo(dataSize);

      Map<String, String> scanned = new HashMap<>();
      for (MapScanCursor<String, String> cursor = redis.hscan("hscan-test"); ; cursor = redis.hscan("hscan-test", cursor)) {
         scanned.putAll(cursor.getMap());
         if (cursor.isFinished()) break;
      }

      assertThat(scanned)
            .hasSize(dataSize)
            .containsAllEntriesOf(content);

      MapScanCursor<String, String> empty = redis.hscan("unknown");
      assertThat(empty)
            .satisfies(v -> assertThat(v.isFinished()).isTrue())
            .satisfies(v -> assertThat(v.getMap()).isEmpty());
   }

   public void testHScanCount() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Map<String, String> content = new HashMap<>();

      int dataSize = 15;
      for (int i = 0; i < dataSize; i++) {
         content.put("key" + i, "value" + i);
      }

      assertThat(redis.hmset("hscan-count-test", content)).isEqualTo("OK");
      assertThat(redis.hlen("hscan-count-test")).isEqualTo(dataSize);

      int count = 5;
      Map<String, String> scanned = new HashMap<>();
      ScanArgs args = ScanArgs.Builder.limit(count);
      for (MapScanCursor<String, String> cursor = redis.hscan("hscan-count-test", args); ; cursor = redis.hscan("hscan-count-test", cursor, args)) {
         scanned.putAll(cursor.getMap());
         if (cursor.isFinished()) break;

         assertThat(cursor.getMap()).hasSize(count);
      }

      assertThat(scanned)
            .hasSize(dataSize)
            .containsAllEntriesOf(content);
   }

   public void testHScanMatch() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Map<String, String> content = new HashMap<>();

      int dataSize = 15;
      for (int i = 0; i < dataSize; i++) {
         content.put("k" + i, "value" + i);
      }

      assertThat(redis.hmset("hscan-match-test", content)).isEqualTo("OK");
      assertThat(redis.hlen("hscan-match-test")).isEqualTo(dataSize);

      Map<String, String> scanned = new HashMap<>();
      ScanArgs args = ScanArgs.Builder.matches("k1*");
      for (MapScanCursor<String, String> cursor = redis.hscan("hscan-match-test", args); ; cursor = redis.hscan("hscan-match-test", cursor, args)) {
         scanned.putAll(cursor.getMap());
         for (String key : cursor.getMap().keySet()) {
            assertThat(key).startsWith("k1");
         }

         if (cursor.isFinished()) break;
      }

      assertThat(scanned)
            .hasSize(6)
            .containsKeys("k1", "k10", "k11", "k12", "k13", "k14");
   }
}
