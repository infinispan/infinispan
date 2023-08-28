package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;

import java.util.HashSet;

import org.testng.annotations.Test;

import io.lettuce.core.ScanArgs;
import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "server.resp.RespSetCommandsTest")
public class RespSetCommandsTest extends SingleNodeRespBaseTest {
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

      // intersect non existent sets returns empty set
      assertThat(redis.sinter("nonexistent", "nonexistent1")).isEmpty();
      assertThat(redis.sinter(key, key1, "nonexistent")).isEmpty();

      // SINTER on an existing key that contains a String, not a Set!
      // Set a String Command
      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.sinter("leads", key));
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

      assertThat(redis.sinterstore("destination", "nonexistent", "nonexistent1")).isEqualTo(0);
      assertThat(redis.smembers("destination")).isEmpty();

      // SINTERSTORE on an existing key that contains a String, not a Set!
      // Set a String Command
      assertWrongType(() -> redis.set("leads", "tristan"), () -> redis.sinterstore("destination", "leads", key));
      // SINTERSTORE on an existing key that contains a List, not a Set!
      // Create a List
      assertWrongType(() -> redis.rpush("listleads", "tristan"),
            () -> redis.sinterstore("destination", "listleads", "william"));
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

   @Test
   public void testSrem() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "srem";
      redis.sadd(key, "1", "2", "3", "4", "5");

      // Remove 1 element
      Long removed = redis.srem(key, "1");
      assertThat(removed.longValue()).isEqualTo(1L);
      // Remove more elements
      removed = redis.srem(key, "4","2","5");
      assertThat(removed.longValue()).isEqualTo(3L);
      // Try removing 1 non present element
      removed = redis.srem(key, "6");
      assertThat(removed.longValue()).isEqualTo(0L);
      // Try removing more non present elements
      removed = redis.srem(key, "6","7");
      assertThat(removed.longValue()).isEqualTo(0L);
      // Some present some not
      removed = redis.srem(key, "3","6");
      assertThat(removed.longValue()).isEqualTo(1L);
      // Set is empty now and has been removed
      assertThat(redis.smembers(key)).isEmpty();
      ScanArgs args = ScanArgs.Builder.matches("k1*");
      var cursor = redis.scan(args);
      assertThat(cursor.getKeys()).doesNotContain(key);

      // Try remove on not existing
      removed = redis.srem(key, "4","2");
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
      assertThat(redis.sunion(key, key1,key2)).containsExactlyInAnyOrder("e1", "e2", "e3", "e4", "e5", "e6");

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
}
