package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

import io.lettuce.core.KeyValue;
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

      assertWrongType(() -> redis.set("plain", "string"), () -> redis.hmset("plain", Map.of("k1", "v1")));
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

      assertWrongType(() -> redis.set("plain", "string"), () -> redis.hmset("plain", Map.of("k1", "v1")));
      assertWrongType(() -> {}, () -> redis.hget("plain", "k1"));
   }

   public void testHashLength() {
      RedisCommands<String, String> redis = redisConnection.sync();

      Map<String, String> map = Map.of("key1", "value1", "key2", "value2", "key3", "value3");
      assertThat(redis.hset("len-test", map)).isEqualTo(3);

      assertThat(redis.hlen("len-test")).isEqualTo(3);
      assertThat(redis.hlen("UNKNOWN")).isEqualTo(0);

      assertWrongType(() -> redis.set("plain", "string"), () -> redis.hlen("plain"));
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

   public void testKeySetOperation() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.hkeys("something")).asList().isEmpty();

      Map<String, String> map = Map.of("key1", "value1", "key2", "value2", "key3", "value3");
      assertThat(redis.hset("keyset-operation", map)).isEqualTo(3);

      assertThat(redis.hkeys("keyset-operation")).asList()
            .hasSize(3).containsAll(map.keySet());

      assertWrongType(() -> redis.set("plain", "string"), () -> redis.hkeys("plain"));
   }

   public void testValuesOperation() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.hvals("something")).asList().isEmpty();

      Map<String, String> map = Map.of("key1", "value1", "key2", "value2", "key3", "value3");
      assertThat(redis.hset("values-operation", map)).isEqualTo(3);

      assertThat(redis.hvals("values-operation")).asList()
            .hasSize(3).containsAll(map.values());

      assertWrongType(() -> redis.set("plain", "string"), () -> redis.hvals("plain"));
   }

   public void testPropertyExists() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.hexists("something", "key")).isFalse();

      assertThat(redis.hset("exists-test", "key", "value")).isTrue();
      assertThat(redis.hexists("exists-test", "key")).isTrue();
      assertThat(redis.hexists("exists-test", "key2")).isFalse();

      assertWrongType(() -> redis.set("plain", "string"), () -> redis.hexists("plain", "key"));
   }

   public void testSetAndGet() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.hdel("not-existent", "key1")).isEqualTo(0);
      assertThat(redis.hgetall("not-existent")).isEmpty();

      Map<String, String> map = Map.of("key1", "value1", "key2", "value2", "key3", "value3");
      assertThat(redis.hset("HSET-HDEL", map))
            .isEqualTo(3);

      assertThat(redis.hgetall("HSET-HDEL")).containsAllEntriesOf(map);

      assertThat(redis.hdel("HSET-HDEL", "key1")).isEqualTo(1);
      assertThat(redis.hdel("HSET-HDEL", "key1")).isEqualTo(0);
      assertThat(redis.hdel("HSET-HDEL", "key2", "key3", "key4")).isEqualTo(2);

      assertThat(redis.hgetall("HSET-HDEL")).isEmpty();

      assertWrongType(() -> redis.set("plain", "string"), () -> redis.hdel("plain", "key1"));
   }

   public void testIncrementOperations() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.hincrby("incr-test", "age", 5)).isEqualTo(5);
      assertThat(redis.hincrby("incr-test", "age", 2)).isEqualTo(7);
      assertThat(redis.hincrby("incr-test", "age", -3)).isEqualTo(4);
      assertThat(redis.hincrby("incr-test", "age", 0)).isEqualTo(4);

      // Now we verify if it is working with additional properties.
      Map<String, String> map = Map.of("key1", "value1");
      assertThat(redis.hset("incr-test", map)).isEqualTo(1);

      assertThat(redis.hget("incr-test", "key1")).isEqualTo("value1");
      assertThat(redis.hget("incr-test", "age")).isEqualTo("4");

      // Incr does not work with something that is not a long.
      assertThatThrownBy(() -> redis.hincrby("incr-test", "key1", 1))
            .hasCauseInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("value is not an integer or out of range");

      assertThat(redis.hincrbyfloat("incr-test", "age", 0.5)).isEqualTo(4.5);

      // Incrbyfloat only work with numbers.
      assertThatThrownBy(() -> redis.hincrbyfloat("incr-test", "key1", 1))
            .hasCauseInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("value is not an integer or out of range");

      // Since the value has an increment of 0.5, we can't use hincrby anymore, the value is not a long.
      assertThatThrownBy(() -> redis.hincrby("incr-test", "age", 1))
            .hasCauseInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("value is not an integer or out of range");

      // Increment by 0.5 leaves with a long value again.
      assertThat(redis.hincrbyfloat("incr-test", "age", 0.5)).isEqualTo(5);
      assertThat(redis.hincrby("incr-test", "age", -1)).isEqualTo(4);
      assertThat(redis.hincrbyfloat("incr-test", "age", -0.5)).isEqualTo(3.5);


      assertThat(redis.hget("incr-test", "key1")).isEqualTo("value1");
      assertThat(redis.hget("incr-test", "age")).isEqualTo("3.5");
   }

   public void testHrandField() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.hrandfield("something")).isNull();

      Map<String, String> map = Map.of("key1", "value1", "key2", "value2", "key3", "value3");
      assertThat(redis.hset("hrand-operations", map)).isEqualTo(3);

      assertThat(redis.hrandfield("hrand-operations")).isIn(map.keySet());
      assertThat(redis.hrandfieldWithvalues("hrand-operations"))
            .satisfies(kv -> assertThat(map.get(kv.getKey())).isEqualTo(kv.getValue()));

      assertThat(redis.hrandfield("hrand-operations", 0)).isEmpty();

      assertThat(redis.hrandfield("hrand-operations", 2)).hasSize(2);
      assertThat(redis.hrandfieldWithvalues("hrand-operations", 2)).hasSize(2);
      assertThat(redis.hrandfield("hrand-operations", 20)).hasSize(3);
      assertThat(redis.hrandfieldWithvalues("hrand-operations", 20)).hasSize(3);

      assertThat(redis.hrandfield("hrand-operations", -20)).hasSize(20);
      assertThat(redis.hrandfieldWithvalues("hrand-operations", -20)).hasSize(20);

      assertWrongType(() -> redis.set("plain", "string"), () -> redis.hrandfield("plain"));
   }

   public void testHMultiGet() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.hmget("something", "k1"))
            .hasSize(1)
            .contains(KeyValue.empty("k1"));

      Map<String, String> map = Map.of("key1", "value1", "key2", "value2", "key3", "value3");
      assertThat(redis.hset("hmget-operations", map)).isEqualTo(3);

      assertThat(redis.hmget("hmget-operations", "key1", "key2", "key3"))
            .hasSize(3)
            .satisfies(entries -> entries.forEach(kv -> assertThat(map.get(kv.getKey())).isEqualTo(kv.getValue())));

      assertThat(redis.hmget("hmget-operations", "key3", "key4"))
            .hasSize(2)
            .contains(KeyValue.just("key3", "value3"))
            .contains(KeyValue.empty("key4"));
   }
}
