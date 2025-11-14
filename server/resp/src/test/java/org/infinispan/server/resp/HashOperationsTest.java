package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.infinispan.server.resp.test.RespTestingUtil.ADMIN;
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;

import java.util.HashMap;
import java.util.Map;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.security.Security;
import org.testng.annotations.Test;

import io.lettuce.core.CopyArgs;
import io.lettuce.core.KeyValue;
import io.lettuce.core.MapScanCursor;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "server.resp.HashOperationsTest")
public class HashOperationsTest extends SingleNodeRespBaseTest {

   @Override
   public Object[] factory() {
      return new Object[] {
            new HashOperationsTest(),
            new HashOperationsTest().withAuthorization(),
      };
   }

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
      assertWrongType(() -> redis.hmset("data", Map.of("k1", "v1")), () -> redis.get("data"));
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

   @Test
   public void testHashStringLength() {
      RedisCommands<String, String> redis = redisConnection.sync();

      Map<String, String> map = Map.of("key1", "value1", "key2", "value2", "key3", "value3");
      assertThat(redis.hset("len-test", map)).isEqualTo(3);

      assertThat(redis.hstrlen("len-test", "key1")).isEqualTo(6);
      assertThat(redis.hstrlen("UNKNOWN", "key1")).isEqualTo(0);
      assertThat(redis.hstrlen("len-test", "UNKNOWN")).isEqualTo(0);

      assertWrongType(() -> redis.set("plain", "string"), () -> redis.hstrlen("plain", "field"));
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
            .hasMessageContaining("hash value is not a float");

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

      assertThat(redis.hset("incr-test", "spacing", " 1.349893")).isTrue();
      assertThatThrownBy(() -> redis.hincrbyfloat("incr-test", "spacing", 2.5))
            .hasCauseInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("hash value is not a float");

      assertThatThrownBy(() -> redis.hincrbyfloat("incr-test", "age", Double.POSITIVE_INFINITY))
            .hasCauseInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR increment would produce NaN or Infinity");
   }

   public void testIncrementOverflows() {
      RedisCommands<String, String> redis = redisConnection.sync();

      long base = 17179869184L;
      assertThat(redis.hset("incr-over", "v", String.valueOf(base))).isTrue();
      assertThat(redis.hincrby("incr-over", "v", 1)).isEqualTo(base + 1);

      assertThat(redis.hset("incr-over", "v2", String.valueOf(-9223372036854775484L))).isTrue();
      assertThatThrownBy(() -> redis.hincrby("incr-over", "v2", -1000))
            .hasCauseInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("increment or decrement would overflow");
   }

   public void testHrandField() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.hrandfield("something")).isNull();
      assertThat(redis.hrandfield("something", 10)).isEmpty();

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

      assertThat(redis.hrandfield("hrand-operations")).matches(map::containsKey);

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

   public void testHSetNx() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.hsetnx("key", "propK", "propV")).isTrue();
      assertThat(redis.hsetnx("key", "propK", "value")).isFalse();
      assertThat(redis.hget("key", "propK")).isEqualTo("propV");
   }

   public void testCopyHash() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Map<String, String> map = Map.of("f1", "v1", "f2", "v2", "f3", "v3");
      redis.hset("copy-src", map);

      assertThat(redis.copy("copy-src", "copy-dst")).isTrue();
      assertThat(redis.hgetall("copy-dst")).containsAllEntriesOf(map);
   }

   public void testCopyHashNotPresent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.copy("copy-missing-hash", "copy-dst")).isFalse();
   }

   public void testCopyHashToExistingKeyWithoutReplace() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Map<String, String> srcMap = Map.of("f1", "v1", "f2", "v2");
      Map<String, String> dstMap = Map.of("f3", "v3", "f4", "v4");
      redis.hset("copy-src-nr", srcMap);
      redis.hset("copy-dst-nr", dstMap);

      assertThat(redis.copy("copy-src-nr", "copy-dst-nr")).isFalse();
      assertThat(redis.hgetall("copy-dst-nr")).containsAllEntriesOf(dstMap);
   }

   public void testCopyHashToExistingKeyWithReplace() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Map<String, String> srcMap = Map.of("f1", "v1", "f2", "v2");
      Map<String, String> dstMap = Map.of("f3", "v3", "f4", "v4");
      redis.hset("copy-src-r", srcMap);
      redis.hset("copy-dst-r", dstMap);

      var copyArgs = new CopyArgs().replace(true);
      assertThat(redis.copy("copy-src-r", "copy-dst-r", copyArgs)).isTrue();
      Map<String, String> result = redis.hgetall("copy-dst-r");
      assertThat(result).containsAllEntriesOf(srcMap);
      assertThat(result).doesNotContainKeys("f3", "f4");
   }

   public void testCopyHashWithReplaceToNonExistingKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Map<String, String> map = Map.of("f1", "v1", "f2", "v2");
      redis.hset("copy-src-rne", map);

      var copyArgs = new CopyArgs().replace(true);
      assertThat(redis.copy("copy-src-rne", "copy-dst-rne", copyArgs)).isTrue();
      assertThat(redis.hgetall("copy-dst-rne")).containsAllEntriesOf(map);
   }

   public void testCopyHashToNewDB() {
      RedisCommands<String, String> redis = redisConnection.sync();
      ConfigurationBuilder builder = defaultRespConfiguration();
      amendConfiguration(builder);

      if (isAuthorizationEnabled()) {
         Security.doAs(ADMIN, () -> {
            manager(0).createCache("1", builder.build());
         });
      } else {
         manager(0).createCache("1", builder.build());
      }

      Map<String, String> map = Map.of("f1", "v1", "f2", "v2");
      redis.hset("copy-hash-db-src", map);

      var copyArgs = new CopyArgs().destinationDb(1);
      assertThat(redis.copy("copy-hash-db-src", "copy-hash-db-dst", copyArgs)).isTrue();
      assertThat(redis.hgetall("copy-hash-db-dst")).isEmpty();

      redis.select(1);
      assertThat(redis.hgetall("copy-hash-db-dst")).containsAllEntriesOf(map);
      redis.select(0);
   }

   public void testCopyHashToNewDBWithReplace() {
      RedisCommands<String, String> redis = redisConnection.sync();
      ConfigurationBuilder builder = defaultRespConfiguration();
      amendConfiguration(builder);

      Map<String, String> srcMap = Map.of("f1", "v1", "f2", "v2");
      Map<String, String> dstMap = Map.of("f3", "v3");
      redis.hset("copy-hash-dbr-src", srcMap);

      redis.select(1);
      redis.hset("copy-hash-dbr-dst", dstMap);
      redis.select(0);

      var copyArgs = new CopyArgs().destinationDb(1).replace(true);
      assertThat(redis.copy("copy-hash-dbr-src", "copy-hash-dbr-dst", copyArgs)).isTrue();

      redis.select(1);
      Map<String, String> result = redis.hgetall("copy-hash-dbr-dst");
      assertThat(result).containsAllEntriesOf(srcMap);
      assertThat(result).doesNotContainKeys("f3");
      redis.select(0);
   }

   public void testCopyHashDataIndependence() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.hset("copy-hash-ind-src", Map.of("f1", "v1", "f2", "v2"));

      assertThat(redis.copy("copy-hash-ind-src", "copy-hash-ind-dst")).isTrue();

      // Modifying original does not affect copy
      redis.hset("copy-hash-ind-src", "f1", "modified");
      assertThat(redis.hget("copy-hash-ind-dst", "f1")).isEqualTo("v1");

      // Adding to original does not affect copy
      redis.hset("copy-hash-ind-src", "f3", "v3");
      assertThat(redis.hgetall("copy-hash-ind-dst")).hasSize(2);

      // Deleting original does not affect copy
      redis.del("copy-hash-ind-src");
      assertThat(redis.hgetall("copy-hash-ind-dst")).containsEntry("f1", "v1").containsEntry("f2", "v2");
   }

   public void testCopyHashSameKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.hset("copy-hash-same", Map.of("f1", "v1"));

      assertThat(redis.copy("copy-hash-same", "copy-hash-same")).isFalse();
      assertThat(redis.hgetall("copy-hash-same")).containsEntry("f1", "v1");
   }

   public void testCopyHashToExistingStringWithoutReplace() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.hset("copy-cross-hash-src", Map.of("f1", "v1"));
      redis.set("copy-cross-hash-dst", "existing");

      assertThat(redis.copy("copy-cross-hash-src", "copy-cross-hash-dst")).isFalse();
      assertThat(redis.get("copy-cross-hash-dst")).isEqualTo("existing");
   }

   public void testCopyHashToExistingStringWithReplace() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.hset("copy-cross-hash-r-src", Map.of("f1", "v1", "f2", "v2"));
      redis.set("copy-cross-hash-r-dst", "existing");

      var copyArgs = new CopyArgs().replace(true);
      assertThat(redis.copy("copy-cross-hash-r-src", "copy-cross-hash-r-dst", copyArgs)).isTrue();
      assertThat(redis.hgetall("copy-cross-hash-r-dst")).containsEntry("f1", "v1").containsEntry("f2", "v2");
   }
}
