package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;

import java.util.HashSet;
import java.util.Set;

import org.testng.annotations.Test;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ValueScanCursor;
import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "server.resp.RespSetCommandsTest")
public class RespSetCommandsTest extends SingleNodeRespBaseTest {

   @Override
   public Object[] factory() {
      return new Object[] {
            new RespSetCommandsTest(),
            new RespSetCommandsTest().withAuthorization(),
      };
   }

   @Test
   public void testSadd() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "sadd";
      Long newValue = redis.sadd(key, "1", "2", "3");
      assertThat(newValue.longValue()).isEqualTo(3L);
      newValue = redis.sadd(key, "4", "5");
      assertThat(newValue.longValue()).isEqualTo(2L);
      newValue = redis.sadd(key, "5", "6");
      assertThat(newValue.longValue()).isEqualTo(1L);

      // SADD on an existing key that contains a String, not a Set!
      // Set a String Command
      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.sadd("leads", "william"));
      // SADD on an existing key that contains a List, not a Set!
      // Create a List
      assertWrongType(() -> redis.lpush("listleads", "tristan"), () -> redis.sadd("listleads", "william"));
      assertWrongType(() -> redis.lpush("data", "e1"), () -> redis.get("data"));
   }

   @Test
   public void testSmembers() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "smembers";
      redis.sadd(key, "e1", "e2", "e3");
      assertThat(redis.smembers(key)).containsExactlyInAnyOrder("e1", "e2", "e3");

      assertThat(redis.smembers("nonexistent")).isEmpty();

      // SMEMBER on an existing key that contains a String, not a Set!
      // Set a String Command
      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.smembers("leads"));
      // SMEMBER on an existing key that contains a List, not a Set!
      // Create a List
      assertWrongType(() -> redis.rpush("listleads", "tristan"), () -> redis.smembers("listleads"));
   }

   @Test
   public void testSismember() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "sismember";
      redis.sadd(key, "e1", "e2", "e3");
      assertThat(redis.sismember(key, "e1")).isTrue();
      assertThat(redis.sismember(key, "e4")).isFalse();
      assertThat(redis.sismember("nonexistent-sismember", "e4")).isFalse();

      // SISMEMBER on an existing key that contains a String, not a Set!
      // Set a String Command
      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.sismember("leads", "tristan"));
      // SISMEMBER on an existing key that contains a List, not a Set!
      // Create a List
      assertWrongType(() -> redis.rpush("listleads", "tristan"), () -> redis.sismember("listleads", "tristan"));
   }

   @Test
   public void testSmismember() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "simsmember";
      redis.sadd(key, "e1", "e2", "e3");
      assertThat(redis.smismember(key, "e1", "e4", "e3", "e1")).containsExactly(true, false, true, true);
      assertThat(redis.smismember("notexistent", "e1", "e4", "e3", "e1")).containsExactly(false, false, false, false);
   }

   @Test
   public void testScard() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "smembers";
      redis.sadd(key, "e1", "e2", "e3");
      assertThat(redis.scard(key)).isEqualTo(3);

      assertThat(redis.scard("nonexistent")).isEqualTo(0);

      // SCARD on an existing key that contains a String, not a Set!
      // Set a String Command
      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.scard("leads"));
      // SCARD on an existing key that contains a List, not a Set!
      // Create a List
      assertWrongType(() -> redis.rpush("listleads", "tristan"), () -> redis.scard("listleads"));
   }

   @Test
   public void testSinter() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "sinter";
      redis.sadd(key, "e1", "e2", "e3");
      // sinter with one set returns the set
      assertThat(redis.sinter(key)).containsExactlyInAnyOrder("e1", "e2", "e3");

      String key1 = "sinter1";
      redis.sadd(key1, "e2", "e3", "e4");
      // check intersection between 2 sets
      assertThat(redis.sinter(key, key1)).containsExactlyInAnyOrder("e2", "e3");

      // intersect all non existent sets returns empty set
      assertThat(redis.sinter("nonexistent", "nonexistent1")).isEmpty();
      assertThat(redis.sinter(key, key1, "nonexistent")).isEmpty();

      // SINTER on an existing key that contains a String, not a Set!
      // Set a String Command
      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.sinter("leads", key));
      // SINTER on an existing key that contains a String, not a Set after a missing key
      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.sinter("nonexistent", "leads", key));
      // SINTER on an existing key that contains a List, not a Set!
      // Create a List
      assertWrongType(() -> redis.rpush("listleads", "tristan"), () -> redis.sinter("listleads", "william"));
   }

   @Test
   public void testSintercard() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "sinter";
      redis.sadd(key, "e1", "e2", "e3");
      assertThat(redis.sintercard(key)).isEqualTo(3);

      String key1 = "sinter1";
      redis.sadd(key1, "e2", "e3", "e4");
      assertThat(redis.sintercard(key, key1)).isEqualTo(2);
      assertThat(redis.sintercard(1, key, key1)).isEqualTo(1);

      assertThat(redis.sintercard("nonexistent", "nonexistent1")).isEqualTo(0);
      assertThat(redis.sintercard(key, key1, "nonexistent")).isEqualTo(0);

      // SINTERCARD on an existing key that contains a String, not a Set!
      // Set a String Command
      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.sintercard("leads", key));
      // SINTERCARD on an existing key that contains a List, not a Set!
      // Create a List
      assertWrongType(() -> redis.rpush("listleads", "tristan"), () -> redis.sintercard("listleads", "william"));

      CustomStringCommands command = CustomStringCommands.instance(redisConnection);

      assertThatThrownBy(() -> {
         command.sintercard5Args("0".getBytes(),"a".getBytes(),"b".getBytes(),"c".getBytes(),"d".getBytes());
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageStartingWith("ERR numkeys");

      assertThatThrownBy(() -> {
         command.sintercard5Args("notnum".getBytes(),"a".getBytes(),"b".getBytes(),"c".getBytes(),"d".getBytes());
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageStartingWith("ERR numkeys");

      assertThatThrownBy(() -> {
         command.sintercard5Args("5".getBytes(),"a".getBytes(),"b".getBytes(),"c".getBytes(),"d".getBytes());
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageStartingWith("ERR Number of keys");

      assertThatThrownBy(() -> {
         command.sintercard5Args("1".getBytes(),"a".getBytes(),"b".getBytes(),"c".getBytes(),"d".getBytes());
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageStartingWith("ERR syntax error");

      assertThatThrownBy(() -> {
         command.sintercard5Args("3".getBytes(),"a".getBytes(),"b".getBytes(),"LIMIT".getBytes(),"-1".getBytes());
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageStartingWith("ERR syntax error");

      assertThatThrownBy(() -> {
         redis.sintercard(-1,"a","b");
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageStartingWith("ERR LIMIT can't be negative");
   }

   @Test
   public void testSinterstore() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "sinter";
      redis.sadd(key, "e1", "e2", "e3");
      assertThat(redis.sinterstore("destination", key)).isEqualTo(3);

      String key1 = "sinter1";
      redis.sadd(key1, "e2", "e3", "e4");
      assertThat(redis.sinterstore("destination", key, key1)).isEqualTo(2);
      assertThat(redis.smembers("destination")).containsExactlyInAnyOrder("e2", "e3");

      // Check a missing key
      assertThat(redis.sinterstore("destination", key, "nonexistent")).isEqualTo(0);
      assertThat(redis.smembers("destination")).isEmpty();

      // Check all missing key
      assertThat(redis.sinterstore("destination", "nonexistent", "nonexistent1")).isEqualTo(0);
      assertThat(redis.smembers("destination")).isEmpty();

      // SINTERSTORE on an existing key that contains a String, not a Set!
      // Set a String Command
      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.sinterstore("destination", "leads", key));
      // SINTERSTORE on an existing key that contains a List, not a Set!
      // Create a List
      assertWrongType(() -> redis.rpush("listleads", "tristan"),
            () -> redis.sinterstore("destination", "listleads", "nonexistent"));
      // Check wrong type after a missing key
      assertWrongType(() -> redis.rpush("listleads", "tristan"),
            () -> redis.sinterstore("destination", "nonexistent", "listleads"));
   }

   @Test
   public void testSmove() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String src = "smove-src";
      String dst = "smove-dst";
      redis.sadd(src, "1", "2", "3");
      redis.sadd(dst, "4", "5");
      assertThat(redis.smove(src, dst, "2")).isTrue();

      assertThat(redis.smembers(src)).containsExactlyInAnyOrder("1", "3");
      assertThat(redis.smembers(dst)).containsExactlyInAnyOrder("2", "4", "5");

      assertThat(redis.smove(src, dst, "3")).isTrue();
      assertThat(redis.smove(src, dst, "3")).isFalse();

      String nesrc = "smove-nonexist-src";
      assertThat(redis.smove(nesrc, dst, "2")).isFalse();

      String nedst = "smove-nonexist-dst";
      assertThat(redis.smove(src, nedst, "1")).isTrue();
      assertThat(redis.smembers(src)).isEmpty();
      assertThat(redis.smembers(nedst)).containsExactlyInAnyOrder("1");

      String samesrc = "same-src";
      redis.sadd(samesrc, "1", "2", "3");
      assertThat(redis.smove(samesrc, samesrc, "2")).isTrue();
      assertThat(redis.smove(samesrc, samesrc, "4")).isFalse();
   }

   public void testSmoveFailures() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.sadd("smove-failures", "1", "2", "3");
      assertWrongType(() -> redis.set("x", "10"), () -> redis.smove("smove-failures", "x", "foo"));
   }

   @Test
   public void testSrem() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "srem";
      redis.sadd(key, "1", "2", "3", "4", "5");

      // Remove 1 element
      Long removed = redis.srem(key, "1");
      assertThat(removed.longValue()).isEqualTo(1L);
      // Remove more elements
      removed = redis.srem(key, "4", "2", "5");
      assertThat(removed.longValue()).isEqualTo(3L);
      // Try removing 1 non present element
      removed = redis.srem(key, "6");
      assertThat(removed.longValue()).isEqualTo(0L);
      // Try removing more non present elements
      removed = redis.srem(key, "6", "7");
      assertThat(removed.longValue()).isEqualTo(0L);
      // Some present some not
      removed = redis.srem(key, "3", "6");
      assertThat(removed.longValue()).isEqualTo(1L);
      // Set is empty now and has been removed
      assertThat(redis.smembers(key)).isEmpty();
      ScanArgs args = ScanArgs.Builder.matches("k1*");
      var cursor = redis.scan(args);
      assertThat(cursor.getKeys()).doesNotContain(key);

      // Try remove on not existing
      removed = redis.srem(key, "4", "2");
      assertThat(removed.longValue()).isEqualTo(0L);

      // SREM removed the entry, since set is empty. Test that key is free
      assertThat(redis.lpush(key, "vittorio")).isEqualTo(1L);

      // SADD on an existing key that contains a String, not a Set!
      // Set a String Command
      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.srem("leads", "william"));
      // SADD on an existing key that contains a List, not a Set!
      // Create a List
      assertWrongType(() -> redis.lpush("listleads", "tristan"), () -> redis.srem("listleads", "william"));
   }

   @Test
   public void testSunion() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "sunion";
      String key1 = "sunion1";
      String key2 = "sunion2";

      redis.sadd(key, "e1");
      // sunion with one set returns the set
      assertThat(redis.sunion(key)).containsExactlyInAnyOrder("e1");

      redis.sadd(key1, "e2", "e3", "e4");
      redis.sadd(key2, "e5", "e6");

      // check union between 2 sets
      assertThat(redis.sunion(key, key1)).containsExactlyInAnyOrder("e1", "e2", "e3", "e4");

      // check union between 3 sets
      assertThat(redis.sunion(key, key1, key2)).containsExactlyInAnyOrder("e1", "e2", "e3", "e4", "e5", "e6");

      // Union non existent sets returns the set
      assertThat(redis.sunion(key1, "nonexistent1")).containsExactlyInAnyOrder("e2", "e3", "e4");
      assertThat(redis.sunion("nonexistent", "nonexistent1")).isEmpty();

      // Union of set with itself returns the set
      assertThat(redis.sunion(key1, key1)).containsExactlyInAnyOrder("e2", "e3", "e4");

      // SUNION on an existing key that contains a String, not a Set!
      // Set a String Command
      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.sunion("leads", key));
      // SUNION on an existing key that contains a List, not a Set!
      // Create a List
      assertWrongType(() -> redis.rpush("listleads", "tristan"), () -> redis.sunion("listleads", "william"));
   }

   @Test
   public void testSunionstore() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "sunionstore";
      redis.sadd(key, "e1", "e2", "e3");
      assertThat(redis.sunionstore("destination", key)).isEqualTo(3);

      String key1 = "sunionstore1";
      redis.sadd(key1, "e2", "e3", "e4");
      assertThat(redis.sunionstore("destination", key, key1)).isEqualTo(4);
      assertThat(redis.smembers("destination")).containsExactlyInAnyOrder("e1", "e2", "e3", "e4");
      assertThat(redis.sunionstore("destination", "destination", "nonexistent1")).isEqualTo(4);
      assertThat(redis.sunionstore("destination", "nonexistent", "nonexistent1")).isEqualTo(0);
      assertThat(redis.smembers("destination")).isEmpty();

      // SUNIONSTORE on an existing key that contains a String, not a Set!
      // Set a String Command
      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.sunionstore("destination", "leads", key));
      // SUNIONSTORE on an existing key that contains a List, not a Set!
      // Create a List
      assertWrongType(() -> redis.rpush("listleads", "tristan"),
            () -> redis.sunionstore("destination", "listleads", "william"));
   }

   @Test
   public void testSpop() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "spop";

      // Test count > size
      redis.sadd(key, "1", "2", "3");
      var initialSet = redis.smembers(key);
      var popSet = redis.spop(key, 4);
      var finalSet = redis.smembers(key);

      assertThat(popSet).containsExactlyInAnyOrderElementsOf(initialSet);
      assertThat(finalSet).isEmpty();

      // Test count = size
      redis.sadd(key, "1", "2", "3", "4");
      initialSet = redis.smembers(key);
      popSet = redis.spop(key, 4);
      finalSet = redis.smembers(key);

      assertThat(popSet).containsExactlyInAnyOrderElementsOf(initialSet);
      assertThat(finalSet).isEmpty();

      // Test count < size
      redis.sadd(key, "1", "2", "3", "4", "5");
      initialSet = redis.smembers(key);
      popSet = redis.spop(key, 3);
      finalSet = redis.smembers(key);

      // Check resulting sets are a partition of initial
      assertThat(popSet.size() + finalSet.size()).isEqualTo(initialSet.size());
      var copyOfPop = new HashSet<>(popSet);
      popSet.addAll(finalSet);
      assertThat(popSet).containsExactlyInAnyOrder(initialSet.toArray(String[]::new));
      initialSet.removeAll(finalSet);
      assertThat(initialSet).containsExactlyInAnyOrder(copyOfPop.toArray(String[]::new));

      // SPOP on an existing key that contains a String, not a Set!
      // Set a String Command
      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.spop("leads", 1));
      // SPOP on an existing key that contains a List, not a Set!
      // Create a List
      assertWrongType(() -> redis.lpush("listleads", "tristan"), () -> redis.spop("listleads", 1));
   }

   @Test
   public void testRandmemberMIN_VALUE() {
      // Testing special case count = Long.MIN_VAL
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "srandmember";
      redis.sadd(key, "1", "2", "3");
      assertThatThrownBy(() -> {
         redis.srandmember(key, Long.MIN_VALUE);
      }).isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR value is out of range");
   }

   @Test
   public void testRandmember() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "srandmember";
      // Test count > size
      redis.sadd(key, "1", "2", "3");
      var initialSet = redis.smembers(key);
      var popSet = redis.srandmember(key, 4);
      var finalSet = redis.smembers(key);
      assertThat(popSet).containsExactlyInAnyOrderElementsOf(initialSet);
      assertThat(finalSet).containsExactlyInAnyOrderElementsOf(initialSet);
      // Test count = size
      popSet = redis.srandmember(key, 4);
      finalSet = redis.smembers(key);
      assertThat(popSet).containsExactlyInAnyOrderElementsOf(initialSet);
      assertThat(finalSet).containsExactlyInAnyOrderElementsOf(initialSet);
      // Test count < size
      redis.sadd(key, "1", "2", "3", "4", "5");
      initialSet = redis.smembers(key);
      popSet = redis.srandmember(key, 3);
      finalSet = redis.smembers(key);
      assertThat(initialSet).containsAll(popSet);

      // Test count < 0
      redis.sadd(key, "1", "2", "3", "4", "5");
      initialSet = redis.smembers(key);
      popSet = redis.srandmember(key, -20);
      assertThat(popSet.size()).isEqualTo(20);
      // Check resulting collection contains only element from intial set
      assertThat(initialSet).containsAll(popSet);

      // SPOP on an existing key that contains a String, not a Set!
      // Set a String Command
      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.srandmember("leads", 1));
      // SPOP on an existing key that contains a List, not a Set!
      // Create a List
      assertWrongType(() -> redis.lpush("listleads", "tristan"), () -> redis.srandmember("listleads", 1));
   }

   @Test
   public void testSscanMatch() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Set<String> content = new HashSet<>();

      int dataSize = 15;
      for (int i = 0; i < dataSize; i++) {
         content.add("v" + i);
      }

      assertThat(redis.sadd("sscan-match-test", content.toArray(String[]::new))).isEqualTo(dataSize);

      Set<String> scanned = new HashSet<>();
      ScanArgs args = ScanArgs.Builder.matches("v1*");
      for (ValueScanCursor<String> cursor = redis.sscan("sscan-match-test", args); ; cursor = redis.sscan("sscan-match-test", cursor, args)) {
         scanned.addAll(cursor.getValues());
         for (String key : cursor.getValues()) {
            assertThat(key).startsWith("v1");
         }

         if (cursor.isFinished()) break;
      }

      assertThat(scanned)
            .hasSize(6)
            .containsExactlyInAnyOrder("v1", "v10", "v11", "v12", "v13", "v14");
   }

   @Test
   public void testHScanOperation() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Set<String> content = new HashSet<>();

      int dataSize = 15;
      redis.flushdb();
      for (int i = 0; i < dataSize; i++) {
         content.add("value" + i);
      }

      assertThat(redis.sadd("sscan-test", content.toArray(String[]::new))).isEqualTo(dataSize);

      Set<String> scanned = new HashSet<>();
      for (ValueScanCursor<String> cursor = redis.sscan("sscan-test"); ; cursor = redis.sscan("sscan-test", cursor)) {
         scanned.addAll(cursor.getValues());
         if (cursor.isFinished()) break;
      }

      assertThat(scanned)
            .hasSize(dataSize)
            .containsExactlyInAnyOrderElementsOf(content);

      ValueScanCursor<String> empty = redis.sscan("unknown");
      assertThat(empty)
            .satisfies(v -> assertThat(v.isFinished()).isTrue())
            .satisfies(v -> assertThat(v.getValues()).isEmpty());
   }

   @Test
   public void testSscanCount() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Set<String> content = new HashSet<>();

      int dataSize = 15;
      for (int i = 0; i < dataSize; i++) {
         content.add("value" + i);
      }

      assertThat(redis.sadd("sscan-count-test", content.toArray(String[]::new))).isEqualTo(dataSize);

      int count = 5;
      Set<String> scanned = new HashSet<>();
      ScanArgs args = ScanArgs.Builder.limit(count);
      for (ValueScanCursor<String> cursor = redis.sscan("sscan-count-test", args); ; cursor = redis.sscan("sscan-count-test", cursor, args)) {
         scanned.addAll(cursor.getValues());
         if (cursor.isFinished()) break;

         assertThat(cursor.getValues()).hasSize(count);
      }

      assertThat(scanned)
            .hasSize(dataSize)
            .containsExactlyInAnyOrderElementsOf(content);
   }

   @Test
   public void testSdiff() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "sdiff";
      redis.sadd(key, "e1", "e2", "e3");

      // check sdiff 2 sets
      String key1 = "sdiff1";
      redis.sadd(key1, "e2", "e3", "e4");
      assertThat(redis.sdiff(key, key1)).containsExactlyInAnyOrder("e1");

      // sdiff 3 sets
      String key2 = "sdiff2";
      redis.sadd(key2, "e1", "e3", "e4");
      assertThat(redis.sdiff(key, key1, key2)).isEmpty();

      // sdiff with itself return empty set
      assertThat(redis.sdiff(key, key)).isEmpty();

      // sdiff with empty return the set
      assertThat(redis.sdiff(key, "nonexistent1"))
            .containsExactlyInAnyOrderElementsOf(redis.smembers(key));

      // sdiff non existent sets returns empty set
      assertThat(redis.sdiff("nonexistent", "nonexistent1")).isEmpty();

      // SDIFF on an existing key that contains a String, not a Set!
      // Set a String Command
      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.sdiff("leads", key));
      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.sdiff(key, "leads"));
      // SDIFF on an existing key that contains a List, not a Set!
      // Create a List
      assertWrongType(() -> redis.rpush("listleads", "tristan"), () -> redis.sdiff("listleads", "william"));
   }

   @Test
   public void testSdiffstore() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String dest = "dest";
      String key = "sdiffStore";
      redis.sadd(key, "e1", "e2", "e3");

      // check sdiff 2 sets
      String key1 = "sdiff1Store";
      redis.sadd(key1, "e2", "e3", "e4");
      assertThat(redis.sdiffstore(dest, key, key1)).isEqualTo(1);
      assertThat(redis.smembers(dest)).containsExactlyInAnyOrder("e1");

      // sdiff 3 sets
      String key2 = "sdiff2Store";
      redis.sadd(key2, "e1", "e3", "e4");
      assertThat(redis.sdiffstore(dest, key, key1, key2)).isEqualTo(0);
      assertThat(redis.smembers(dest)).isEmpty();

      // sdiff with itself return empty set
      assertThat(redis.sdiffstore(dest, key, key)).isEqualTo(0);
      assertThat(redis.smembers(dest)).isEmpty();

      // sdiff with empty return the set
      assertThat(redis.sdiffstore(dest, key, "nonexistent1"))
            .isEqualTo(redis.smembers(key).size());
      assertThat(redis.smembers(dest))
            .containsExactlyInAnyOrderElementsOf(redis.smembers(key));

      // sdiff non existent sets returns empty set
      assertThat(redis.sdiffstore("dest", "nonexistent", "nonexistent1")).isEqualTo(0);
      assertThat(redis.smembers(dest)).isEmpty();

      // SDIFF on an existing key that contains a String, not a Set!
      // Set a String Command
      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.sdiffstore("dest", "leads", key));
      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.sdiffstore("dest", key, "leads"));
      // SDIFF on an existing key that contains a List, not a Set!
      // Create a List
      assertWrongType(() -> redis.rpush("listleads", "tristan"),
            () -> redis.sdiffstore("dest", "listleads", "william"));
   }

   public void testSaddDuplicates() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "sadd-dup";
      assertThat(redis.sadd(key, "a", "b", "c")).isEqualTo(3);
      // Adding existing elements returns 0
      assertThat(redis.sadd(key, "a", "b")).isEqualTo(0);
      // Mixed: some new, some existing
      assertThat(redis.sadd(key, "b", "c", "d", "e")).isEqualTo(2);
      assertThat(redis.scard(key)).isEqualTo(5);
   }

   public void testSremDestroysKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "srem-destroy";
      redis.sadd(key, "a", "b");
      // Remove more elements than exist
      assertThat(redis.srem(key, "a", "b", "c", "d")).isEqualTo(2);
      // Key should be removed
      assertThat(redis.exists(key)).isEqualTo(0);
   }

   public void testSpopSingle() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "spop-single";
      redis.sadd(key, "a", "b", "c");
      Set<String> initial = redis.smembers(key);

      // SPOP without count returns one element
      String popped = redis.spop(key);
      assertThat(initial).contains(popped);
      assertThat(redis.scard(key)).isEqualTo(2);

      // SPOP on non-existing key returns nil
      assertThat(redis.spop("nokey")).isNull();
   }

   public void testSpopCountZero() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "spop-zero";
      redis.sadd(key, "a", "b", "c");
      // SPOP with count 0 returns empty set
      assertThat(redis.spop(key, 0)).isEmpty();
      // Set should be unchanged
      assertThat(redis.scard(key)).isEqualTo(3);
   }

   public void testSpopCountOne() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "spop-one";
      redis.sadd(key, "a", "b", "c");
      Set<String> initial = redis.smembers(key);
      Set<String> popped = redis.spop(key, 1);
      assertThat(popped).hasSize(1);
      assertThat(initial).containsAll(popped);
      assertThat(redis.scard(key)).isEqualTo(2);
   }

   public void testSpopNonExistingKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.spop("spop-nokey")).isNull();
      assertThat(redis.spop("spop-nokey", 5)).isEmpty();
   }

   public void testSrandmemberCountZero() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "srand-zero";
      redis.sadd(key, "a", "b", "c");
      assertThat(redis.srandmember(key, 0)).isEmpty();
   }

   public void testSrandmemberNonExistingKeyWithCount() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.srandmember("srand-nokey", 5)).isEmpty();
      assertThat(redis.srandmember("srand-nokey", -5)).isEmpty();
   }

   public void testSrandmemberSingle() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "srand-single";
      redis.sadd(key, "a", "b", "c");
      // Single SRANDMEMBER
      String result = redis.srandmember(key);
      assertThat(result).isIn("a", "b", "c");
      // Set should be unchanged
      assertThat(redis.scard(key)).isEqualTo(3);
   }

   public void testSrandmemberSingleNonExisting() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.srandmember("srand-single-nokey")).isNull();
   }

   public void testSdiffWithFirstSetEmpty() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.sadd("sdiff-b", "a", "b", "c");
      // Diff with empty first set returns empty
      assertThat(redis.sdiff("nokey", "sdiff-b")).isEmpty();
   }

   public void testSinterThreeSets() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.sadd("si3-1", "a", "b", "c", "d");
      redis.sadd("si3-2", "b", "c", "d", "e");
      redis.sadd("si3-3", "c", "d", "e", "f");
      assertThat(redis.sinter("si3-1", "si3-2", "si3-3")).containsExactlyInAnyOrder("c", "d");
   }

   public void testSmoveWrongSrcType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set("str-key", "value");
      redis.sadd("smove-wt-dst", "a");
      assertWrongType(() -> {}, () -> redis.smove("str-key", "smove-wt-dst", "value"));
   }

   public void testSmismemberWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertWrongType(() -> redis.set("plain", "value"), () -> redis.smismember("plain", "a"));
   }

   public void testSscanWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertWrongType(() -> redis.set("plain", "value"), () -> redis.sscan("plain"));
   }

   public void testSinterWithNonExistingKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.sadd("si-exist", "a", "b", "c");
      // Intersection with non-existing key is always empty
      assertThat(redis.sinter("si-exist", "nokey")).isEmpty();
   }

   public void testSunionstoreNonExistingKeys() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // Union of non-existing keys
      assertThat(redis.sunionstore("su-dst", "nokey1", "nokey2")).isEqualTo(0);
      assertThat(redis.exists("su-dst")).isEqualTo(0);
   }

   public void testSdiffstoreNonExistingKeys() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.sdiffstore("sd-dst", "nokey1", "nokey2")).isEqualTo(0);
      assertThat(redis.exists("sd-dst")).isEqualTo(0);
   }

   public void testSinterstoreNonExistingKeys() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.sinterstore("sis-dst", "nokey1", "nokey2")).isEqualTo(0);
      assertThat(redis.exists("sis-dst")).isEqualTo(0);
   }

   public void testSremNonExistingKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.srem("srem-nokey", "a", "b")).isEqualTo(0);
   }

   public void testSaddLargeSet() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "sadd-large";
      String[] values = new String[500];
      for (int i = 0; i < 500; i++) {
         values[i] = "elem" + i;
      }
      assertThat(redis.sadd(key, values)).isEqualTo(500);
      assertThat(redis.scard(key)).isEqualTo(500);
      assertThat(redis.sismember(key, "elem0")).isTrue();
      assertThat(redis.sismember(key, "elem499")).isTrue();
      assertThat(redis.sismember(key, "elem500")).isFalse();
   }

   public void testSmoveNonExistingElement() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.sadd("smove-ne-src", "a", "b");
      redis.sadd("smove-ne-dst", "c");
      // Move non-existing element returns false
      assertThat(redis.smove("smove-ne-src", "smove-ne-dst", "z")).isFalse();
      // Both sets unchanged
      assertThat(redis.smembers("smove-ne-src")).containsExactlyInAnyOrder("a", "b");
      assertThat(redis.smembers("smove-ne-dst")).containsExactlyInAnyOrder("c");
   }

   public void testSmoveToNonExistingDst() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.sadd("smove-newdst-src", "a", "b");
      assertThat(redis.smove("smove-newdst-src", "smove-newdst-dst", "a")).isTrue();
      assertThat(redis.smembers("smove-newdst-src")).containsExactlyInAnyOrder("b");
      assertThat(redis.smembers("smove-newdst-dst")).containsExactlyInAnyOrder("a");
   }

   public void testSrandmemberNoDuplicatesPositiveCount() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "srand-nodup";
      redis.sadd(key, "a", "b", "c", "d", "e");
      // Positive count: distinct results
      var result = redis.srandmember(key, 5);
      assertThat(result).hasSize(5).doesNotHaveDuplicates();
   }

   public void testSrandmemberAllowsDuplicatesNegativeCount() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "srand-dup";
      redis.sadd(key, "only");
      // Negative count with single element: duplicates allowed
      var result = redis.srandmember(key, -10);
      assertThat(result).hasSize(10);
      assertThat(new HashSet<>(result)).containsExactly("only");
   }

   public void testSintercardThreeSets() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.sadd("sic3-1", "a", "b", "c", "d");
      redis.sadd("sic3-2", "b", "c", "d", "e");
      redis.sadd("sic3-3", "c", "d", "e", "f");
      assertThat(redis.sintercard("sic3-1", "sic3-2", "sic3-3")).isEqualTo(2);
      // With limit
      assertThat(redis.sintercard(1, "sic3-1", "sic3-2", "sic3-3")).isEqualTo(1);
   }

   public void testSinterSunionSdiffSameSets() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.sadd("same-set", "a", "b", "c");

      // SINTER with three copies of the same set returns the set itself
      assertThat(redis.sinter("same-set", "same-set", "same-set"))
            .containsExactlyInAnyOrder("a", "b", "c");

      // SUNION with three copies of the same set returns the set itself
      assertThat(redis.sunion("same-set", "same-set", "same-set"))
            .containsExactlyInAnyOrder("a", "b", "c");

      // SDIFF with three copies of the same set returns empty
      assertThat(redis.sdiff("same-set", "same-set", "same-set")).isEmpty();
   }

   public void testSmoveElementAlreadyInDst() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.sadd("smove-dup-src", "a", "b", "c");
      redis.sadd("smove-dup-dst", "b", "d");

      // SMOVE an element that already exists in dst
      assertThat(redis.smove("smove-dup-src", "smove-dup-dst", "b")).isTrue();
      assertThat(redis.smembers("smove-dup-src")).containsExactlyInAnyOrder("a", "c");
      assertThat(redis.smembers("smove-dup-dst")).containsExactlyInAnyOrder("b", "d");
      assertThat(redis.scard("smove-dup-dst")).isEqualTo(2);
   }

   public void testSaddLargeIntegers() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // Integers larger than 64 bits stored as strings
      String bigNum = "12345678901234567890123456789";
      assertThat(redis.sadd("sadd-bigint", bigNum)).isEqualTo(1);
      assertThat(redis.sismember("sadd-bigint", bigNum)).isTrue();
      assertThat(redis.smembers("sadd-bigint")).containsExactly(bigNum);
   }

   public void testSinterstoreDeletesDstkey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // Existing dest should be deleted when result is empty
      redis.sadd("sisd-dst", "old1", "old2");
      redis.sadd("sisd-1", "a", "b");
      redis.sadd("sisd-2", "c", "d");
      // Disjoint sets: intersection is empty, dest should be deleted
      assertThat(redis.sinterstore("sisd-dst", "sisd-1", "sisd-2")).isEqualTo(0);
      assertThat(redis.exists("sisd-dst")).isEqualTo(0);
   }

   public void testSunionstoreDeletesDstkey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // Existing dest should be deleted when union of non-existing keys is empty
      redis.sadd("susd-dst", "old1", "old2");
      assertThat(redis.sunionstore("susd-dst", "nokey1", "nokey2")).isEqualTo(0);
      assertThat(redis.exists("susd-dst")).isEqualTo(0);
   }

   public void testSdiffstoreDeletesDstkey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // Existing dest should be deleted when diff result is empty
      redis.sadd("sdsd-dst", "old1", "old2");
      redis.sadd("sdsd-1", "a", "b");
      redis.sadd("sdsd-2", "a", "b", "c");
      assertThat(redis.sdiffstore("sdsd-dst", "sdsd-1", "sdsd-2")).isEqualTo(0);
      assertThat(redis.exists("sdsd-dst")).isEqualTo(0);
   }

   public void testSunionWithNonExistingKeys() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.sadd("su-ne-1", "a", "b");
      // Union with non-existing key should include only existing set's members
      assertThat(redis.sunion("su-ne-1", "su-ne-nokey"))
            .containsExactlyInAnyOrder("a", "b");
      // Union of only non-existing keys is empty
      assertThat(redis.sunion("su-ne-nokey1", "su-ne-nokey2")).isEmpty();
   }

   public void testSdiffNonExistingKeyPositions() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.sadd("sd-ne", "a", "b", "c");
      // Non-existing key as second argument: diff is the full first set
      assertThat(redis.sdiff("sd-ne", "sd-ne-nokey")).containsExactlyInAnyOrder("a", "b", "c");
      // Non-existing key as first argument: diff is empty
      assertThat(redis.sdiff("sd-ne-nokey", "sd-ne")).isEmpty();
   }

   public void testSpopRemovesEmptyKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.sadd("spop-empty", "a");
      redis.spop("spop-empty");
      // Key should be removed after last element popped
      assertThat(redis.exists("spop-empty")).isEqualTo(0);
   }

   public void testSrandmemberCountOverflow() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.sadd("srand-over", "a", "b", "c");
      // Positive count > size: returns entire set with no duplicates
      var result = redis.srandmember("srand-over", 100);
      assertThat(result).hasSize(3).doesNotHaveDuplicates()
            .containsExactlyInAnyOrder("a", "b", "c");
   }

   public void testSremWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertWrongType(() -> redis.set("srem-wt", "value"), () -> redis.srem("srem-wt", "a"));
   }

   public void testSmoveWrongDstType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.sadd("smove-wdt-src", "a", "b");
      assertWrongType(() -> redis.set("smove-wdt-dst", "value"),
            () -> redis.smove("smove-wdt-src", "smove-wdt-dst", "a"));
   }

   public void testSinterstoreThreeSets() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.sadd("sis3-1", "a", "b", "c", "d");
      redis.sadd("sis3-2", "b", "c", "d", "e");
      redis.sadd("sis3-3", "c", "d", "e", "f");
      assertThat(redis.sinterstore("sis3-dst", "sis3-1", "sis3-2", "sis3-3")).isEqualTo(2);
      assertThat(redis.smembers("sis3-dst")).containsExactlyInAnyOrder("c", "d");
   }

   public void testSunionstoreThreeSets() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.sadd("sus3-1", "a", "b");
      redis.sadd("sus3-2", "b", "c");
      redis.sadd("sus3-3", "c", "d");
      assertThat(redis.sunionstore("sus3-dst", "sus3-1", "sus3-2", "sus3-3")).isEqualTo(4);
      assertThat(redis.smembers("sus3-dst")).containsExactlyInAnyOrder("a", "b", "c", "d");
   }

   public void testSdiffstoreThreeSets() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.sadd("sds3-1", "a", "b", "c", "d");
      redis.sadd("sds3-2", "b");
      redis.sadd("sds3-3", "c");
      assertThat(redis.sdiffstore("sds3-dst", "sds3-1", "sds3-2", "sds3-3")).isEqualTo(2);
      assertThat(redis.smembers("sds3-dst")).containsExactlyInAnyOrder("a", "d");
   }

   public void testSmoveSrcBecomesEmpty() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.sadd("smove-rem-src", "only");
      redis.sadd("smove-rem-dst", "x");
      assertThat(redis.smove("smove-rem-src", "smove-rem-dst", "only")).isTrue();
      // Source should be deleted when it becomes empty
      assertThat(redis.exists("smove-rem-src")).isEqualTo(0);
      assertThat(redis.smembers("smove-rem-dst")).containsExactlyInAnyOrder("only", "x");
   }

   @Test
   public void testRemoveEmptySet() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String dest = "dest";
      String key = "key";
      String key1 = "key1";

      // All popped, check set deleted
      redis.sadd(key, "e1", "e2", "e3");
      redis.spop(key,3);
      assertThat(redis.exists(key)).isEqualTo(0L);

      // difference b/w empty sets, check dest deleted/not created
      redis.sadd(dest, "bedeleted");
      redis.sdiffstore(dest, key, key);
      assertThat(redis.exists(dest)).isEqualTo(0L);
      redis.sdiffstore(dest, key, key);
      assertThat(redis.exists(dest)).isEqualTo(0L);

      // difference b/w same set
      redis.sadd(dest, "bedeleted");
      redis.sadd(key, "e1", "e2", "e3");
      redis.sdiffstore(dest, key, key);
      assertThat(redis.exists(dest)).isEqualTo(0L);
      redis.sdiffstore(dest, key, key);
      assertThat(redis.exists(dest)).isEqualTo(0L);
      redis.del(key);

      // union b/w empty sets
      redis.sadd(dest, "bedeleted");
      redis.sunionstore(dest, key, key);
      assertThat(redis.exists(dest)).isEqualTo(0L);
      redis.sunionstore(dest, key, key);
      assertThat(redis.exists(dest)).isEqualTo(0L);
      redis.del(key);

      // inters b/w empty sets
      redis.sadd(dest, "bedeleted");
      redis.sinterstore(dest, key, key);
      assertThat(redis.exists(dest)).isEqualTo(0L);
      redis.sinterstore(dest, key, key);
      assertThat(redis.exists(dest)).isEqualTo(0L);
      redis.del(key);

      // inters b/w disjunct sets
      redis.sadd(dest, "bedeleted");
      redis.sadd(key, "e1", "e2", "e3");
      redis.sadd(key1, "e11", "e21", "e31");
      redis.sinterstore(dest, key, key1);
      assertThat(redis.exists(dest)).isEqualTo(0L);
      redis.sinterstore(dest, key, key1);
      assertThat(redis.exists(dest)).isEqualTo(0L);
      redis.del(key);

   }
}
