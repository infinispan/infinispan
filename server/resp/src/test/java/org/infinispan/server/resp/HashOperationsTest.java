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
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;

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

   public void testHSetWrongNumberOfArgs() {
      RedisCommands<String, String> redis = redisConnection.sync();
      RedisCodec<String, String> codec = StringCodec.UTF8;

      // HSET with odd number of field-value args (missing value)
      assertThatThrownBy(() -> redis.dispatch(CommandType.HSET,
            new IntegerOutput<>(codec),
            new CommandArgs<>(codec).addKey("myhash").add("field1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("wrong number of arguments");

      // HMSET with odd number of field-value args
      assertThatThrownBy(() -> redis.dispatch(CommandType.HMSET,
            new StatusOutput<>(codec),
            new CommandArgs<>(codec).addKey("myhash").add("field1")))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("wrong number of arguments");
   }

   public void testHIncrByNonExistingKey() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // HINCRBY against a fresh key
      assertThat(redis.hincrby("hincrby-fresh", "counter", 10)).isEqualTo(10);
      assertThat(redis.hget("hincrby-fresh", "counter")).isEqualTo("10");
   }

   public void testHIncrByNonExistingField() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.hset("hincrby-field", "existing", "value");
      assertThat(redis.hincrby("hincrby-field", "newfield", 5)).isEqualTo(5);
      assertThat(redis.hget("hincrby-field", "newfield")).isEqualTo("5");
   }

   public void testHIncrByOver32bit() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.hset("hincrby-32", "v", "17179869184");
      assertThat(redis.hincrby("hincrby-32", "v", 1)).isEqualTo(17179869185L);
   }

   public void testHIncrByOver32bitWithOver32bitIncrement() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.hset("hincrby-32b", "v", "17179869184");
      assertThat(redis.hincrby("hincrby-32b", "v", 17179869184L)).isEqualTo(34359738368L);
   }

   public void testHIncrByFailsWithSpaces() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Spaces on the left
      redis.hset("hincrby-sp", "v1", " 11");
      assertThatThrownBy(() -> redis.hincrby("hincrby-sp", "v1", 1))
            .hasCauseInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("value is not an integer");

      // Spaces on the right
      redis.hset("hincrby-sp", "v2", "11 ");
      assertThatThrownBy(() -> redis.hincrby("hincrby-sp", "v2", 1))
            .hasCauseInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("value is not an integer");
   }

   public void testHIncrByOverflowDetection() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Positive overflow
      redis.hset("hincrby-ov", "v1", String.valueOf(Long.MAX_VALUE));
      assertThatThrownBy(() -> redis.hincrby("hincrby-ov", "v1", 1))
            .hasCauseInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("increment or decrement would overflow");

      // Negative overflow
      redis.hset("hincrby-ov", "v2", String.valueOf(Long.MIN_VALUE));
      assertThatThrownBy(() -> redis.hincrby("hincrby-ov", "v2", -1))
            .hasCauseInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("increment or decrement would overflow");
   }

   public void testHIncrByFloatNonExistingKey() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.hincrbyfloat("hfloat-fresh", "field", 2.5)).isEqualTo(2.5);
      assertThat(redis.hget("hfloat-fresh", "field")).isEqualTo("2.5");
   }

   public void testHIncrByFloatNonExistingField() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.hset("hfloat-field", "existing", "value");
      assertThat(redis.hincrbyfloat("hfloat-field", "newfield", 3.14)).isEqualTo(3.14);
   }

   public void testHIncrByFloatOver32bit() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.hset("hfloat-32", "v", "17179869184");
      assertThat(redis.hincrbyfloat("hfloat-32", "v", 1.5)).isEqualTo(17179869185.5);
   }

   public void testHIncrByFloatFailsWithSpaces() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Spaces on the left
      redis.hset("hfloat-sp", "v1", " 2.5");
      assertThatThrownBy(() -> redis.hincrbyfloat("hfloat-sp", "v1", 1.0))
            .hasCauseInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("hash value is not a float");

      // Spaces on the right
      redis.hset("hfloat-sp", "v2", "2.5 ");
      assertThatThrownBy(() -> redis.hincrbyfloat("hfloat-sp", "v2", 1.0))
            .hasCauseInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("hash value is not a float");
   }

   public void testHIncrByFloatNaNNotAllowed() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.hset("hfloat-nan", "v", "0");
      // NaN is rejected - the serialized "NaN" is not parseable as a number
      assertThatThrownBy(() -> redis.hincrbyfloat("hfloat-nan", "v", Double.NaN))
            .hasCauseInstanceOf(RedisCommandExecutionException.class);

      assertThatThrownBy(() -> redis.hincrbyfloat("hfloat-nan", "v", Double.NEGATIVE_INFINITY))
            .hasCauseInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("NaN or Infinity");
   }

   public void testHDelMultipleFields() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.hset("hdel-multi", Map.of("a", "1", "b", "2", "c", "3", "d", "4"));
      // Delete multiple, some existing
      assertThat(redis.hdel("hdel-multi", "a", "c", "nonexist")).isEqualTo(2);
      assertThat(redis.hgetall("hdel-multi")).containsOnlyKeys("b", "d");
   }

   public void testHDelHashBecomesEmpty() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.hset("hdel-empty", Map.of("a", "1", "b", "2"));
      assertThat(redis.hdel("hdel-empty", "a", "b", "c")).isEqualTo(2);
      // Key should be removed when hash becomes empty
      assertThat(redis.exists("hdel-empty")).isEqualTo(0);
   }

   public void testHSetUpdateAndInsert() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // HSET returns count of new fields only
      assertThat(redis.hset("hset-ui", "f1", "v1")).isTrue();
      assertThat(redis.hset("hset-ui", "f1", "updated")).isFalse();
      assertThat(redis.hget("hset-ui", "f1")).isEqualTo("updated");
   }

   public void testHGetNonExistingKey() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.hget("totally-unknown", "field")).isNull();
   }

   public void testHGetAllNonExistingKey() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.hgetall("nonexist-hash")).isEmpty();
   }

   public void testHSetNxWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertWrongType(() -> redis.set("plain", "string"), () -> redis.hsetnx("plain", "field", "value"));
   }

   public void testHIncrByWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertWrongType(() -> redis.set("plain", "string"), () -> redis.hincrby("plain", "field", 1));
   }

   public void testHIncrByFloatWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertWrongType(() -> redis.set("plain", "string"), () -> redis.hincrbyfloat("plain", "field", 1.0));
   }

   public void testHStrLenCornerCases() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.hmset("hstrlen-cc", Map.of(
            "empty", "",
            "short", "a",
            "long", "abcdefghijklmnopqrstuvwxyz",
            "number", "12345",
            "negative", "-1",
            "float", "3.14"
      ));

      assertThat(redis.hstrlen("hstrlen-cc", "empty")).isEqualTo(0);
      assertThat(redis.hstrlen("hstrlen-cc", "short")).isEqualTo(1);
      assertThat(redis.hstrlen("hstrlen-cc", "long")).isEqualTo(26);
      assertThat(redis.hstrlen("hstrlen-cc", "number")).isEqualTo(5);
      assertThat(redis.hstrlen("hstrlen-cc", "negative")).isEqualTo(2);
      assertThat(redis.hstrlen("hstrlen-cc", "float")).isEqualTo(4);
      assertThat(redis.hstrlen("hstrlen-cc", "nonexist")).isEqualTo(0);
   }

   public void testHRandFieldCountZero() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.hset("hrand-zero", Map.of("a", "1", "b", "2"));
      assertThat(redis.hrandfield("hrand-zero", 0)).isEmpty();
      assertThat(redis.hrandfieldWithvalues("hrand-zero", 0)).isEmpty();
   }

   public void testHRandFieldNonExistingKeyWithCount() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.hrandfield("hrand-nonexist", 5)).isEmpty();
      assertThat(redis.hrandfieldWithvalues("hrand-nonexist", 5)).isEmpty();
   }

   public void testHMGetWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertWrongType(() -> redis.set("plain", "string"), () -> redis.hmget("plain", "field"));
   }

   public void testHRandFieldWithNegativeCountDuplicates() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // With a single field, negative count should return duplicates
      redis.hset("hrand-neg", "only", "field");
      assertThat(redis.hrandfield("hrand-neg", -5)).hasSize(5);
      assertThat(redis.hrandfieldWithvalues("hrand-neg", -5)).hasSize(5);
   }

   public void testHRandFieldPositiveCountNoDuplicates() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.hset("hrand-pos", Map.of("a", "1", "b", "2", "c", "3"));
      // Positive count should return distinct fields
      java.util.List<String> result = redis.hrandfield("hrand-pos", 3);
      assertThat(result).hasSize(3).doesNotHaveDuplicates();
      assertThat(result).containsExactlyInAnyOrder("a", "b", "c");
   }

   public void testHScanWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertWrongType(() -> redis.set("plain", "string"), () -> redis.hscan("plain"));
   }

   public void testHashLargeValues() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Test with a large key and value
      String largeValue = "x".repeat(1024);
      redis.hset("large-hash", "field", largeValue);
      assertThat(redis.hget("large-hash", "field")).isEqualTo(largeValue);
      assertThat(redis.hstrlen("large-hash", "field")).isEqualTo(1024);
   }

   public void testHIncrByFloatOver32bitWithOver32bitIncrement() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.hset("hfloat-32b", "v", "17179869184");
      assertThat(redis.hincrbyfloat("hfloat-32b", "v", 17179869184.0)).isEqualTo(34359738368.0);
   }

   public void testHIncrByFloatCorrectRepresentation() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Redis issue #2846: ensure correct float representation
      redis.hset("hfloat-repr", "field", "0");
      assertThat(redis.hincrbyfloat("hfloat-repr", "field", 1.23)).isEqualTo(1.23);
      assertThat(redis.hget("hfloat-repr", "field")).isEqualTo("1.23");

      // Verify integer-like floats are stored correctly
      redis.hset("hfloat-repr", "f2", "10.50");
      assertThat(redis.hincrbyfloat("hfloat-repr", "f2", 0.1)).isEqualTo(10.6);
      assertThat(redis.hget("hfloat-repr", "f2")).isIn("10.6", "10.59999999999999964");
   }

   public void testHashManyFields() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Create a hash with many fields
      Map<String, String> fields = new HashMap<>();
      for (int i = 0; i < 512; i++) {
         fields.put("field:" + i, "value:" + i);
      }
      redis.hset("big-hash", fields);
      assertThat(redis.hlen("big-hash")).isEqualTo(512);

      // Verify all fields retrievable
      assertThat(redis.hgetall("big-hash")).hasSize(512).containsAllEntriesOf(fields);
      assertThat(redis.hkeys("big-hash")).hasSize(512);
      assertThat(redis.hvals("big-hash")).hasSize(512);

      // Verify individual access
      assertThat(redis.hget("big-hash", "field:0")).isEqualTo("value:0");
      assertThat(redis.hget("big-hash", "field:511")).isEqualTo("value:511");
   }

   public void testHMGetWithDuplicateFields() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.hset("hmget-dup", Map.of("a", "1", "b", "2"));
      // HMGET with duplicate fields should return each occurrence
      java.util.List<KeyValue<String, String>> result = redis.hmget("hmget-dup", "a", "b", "a");
      assertThat(result).hasSize(3);
      assertThat(result.get(0).getValue()).isEqualTo("1");
      assertThat(result.get(1).getValue()).isEqualTo("2");
      assertThat(result.get(2).getValue()).isEqualTo("1");
   }

   public void testHSetNxCreatesHash() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // HSETNX on non-existing key creates hash
      assertThat(redis.hsetnx("hsetnx-fresh", "f1", "v1")).isTrue();
      assertThat(redis.hlen("hsetnx-fresh")).isEqualTo(1);
      assertThat(redis.hget("hsetnx-fresh", "f1")).isEqualTo("v1");

      // Second field
      assertThat(redis.hsetnx("hsetnx-fresh", "f2", "v2")).isTrue();
      assertThat(redis.hlen("hsetnx-fresh")).isEqualTo(2);
   }

   public void testHGetAllContentVerification() {
      RedisCommands<String, String> redis = redisConnection.sync();

      Map<String, String> map = Map.of(
            "name", "John",
            "age", "30",
            "city", "London",
            "email", "john@example.com"
      );
      redis.hset("hgetall-verify", map);

      Map<String, String> result = redis.hgetall("hgetall-verify");
      assertThat(result).hasSize(4).containsExactlyInAnyOrderEntriesOf(map);
   }

   public void testHDelSingleField() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.hset("hdel-single", Map.of("a", "1", "b", "2", "c", "3"));

      // Delete existing field returns 1
      assertThat(redis.hdel("hdel-single", "a")).isEqualTo(1);
      assertThat(redis.hexists("hdel-single", "a")).isFalse();

      // Delete non-existing field returns 0
      assertThat(redis.hdel("hdel-single", "nonexist")).isEqualTo(0);

      // Remaining fields intact
      assertThat(redis.hget("hdel-single", "b")).isEqualTo("2");
      assertThat(redis.hget("hdel-single", "c")).isEqualTo("3");
      assertThat(redis.hlen("hdel-single")).isEqualTo(2);
   }

   public void testHSetVariadic() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // HSET with multiple field-value pairs in a single call
      assertThat(redis.hset("hset-var", Map.of("f1", "v1", "f2", "v2", "f3", "v3"))).isEqualTo(3);

      // Update some and add new
      assertThat(redis.hset("hset-var", Map.of("f1", "new1", "f4", "v4"))).isEqualTo(1);
      assertThat(redis.hget("hset-var", "f1")).isEqualTo("new1");
      assertThat(redis.hget("hset-var", "f4")).isEqualTo("v4");
      assertThat(redis.hlen("hset-var")).isEqualTo(4);
   }

   public void testHGetAllWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertWrongType(() -> redis.set("plain", "string"), () -> redis.hgetall("plain"));
   }

   public void testHExistsAfterDel() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.hset("hexists-del", "field", "value");
      assertThat(redis.hexists("hexists-del", "field")).isTrue();

      redis.hdel("hexists-del", "field");
      assertThat(redis.hexists("hexists-del", "field")).isFalse();
   }

   public void testHIncrByCreatedByItself() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // HINCRBY creates the field, then increments it
      assertThat(redis.hincrby("hincrby-self", "counter", 5)).isEqualTo(5);
      assertThat(redis.hincrby("hincrby-self", "counter", 3)).isEqualTo(8);
      assertThat(redis.hincrby("hincrby-self", "counter", -2)).isEqualTo(6);
      assertThat(redis.hget("hincrby-self", "counter")).isEqualTo("6");
   }

   public void testHIncrByAgainstHSetField() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // HINCRBY on a field originally set with HSET
      redis.hset("hincrby-hset", "count", "100");
      assertThat(redis.hincrby("hincrby-hset", "count", 10)).isEqualTo(110);
      assertThat(redis.hget("hincrby-hset", "count")).isEqualTo("110");
   }

   public void testHIncrByFloatCreatedByHIncrBy() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Field created by HINCRBY, then incremented by HINCRBYFLOAT
      redis.hincrby("hfloat-from-int", "v", 10);
      assertThat(redis.hincrbyfloat("hfloat-from-int", "v", 0.5)).isEqualTo(10.5);
      assertThat(redis.hget("hfloat-from-int", "v")).isEqualTo("10.5");
   }

   public void testHIncrByFloatAgainstHSetField() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // HINCRBYFLOAT on a field originally set with HSET
      redis.hset("hfloat-hset", "price", "9.99");
      assertThat(redis.hincrbyfloat("hfloat-hset", "price", 0.01)).isEqualTo(10.0);
   }
}
