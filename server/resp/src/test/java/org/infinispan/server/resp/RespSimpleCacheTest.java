package org.infinispan.server.resp;

import static io.lettuce.core.ScoredValue.just;
import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;

import java.util.Map;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "server.resp.RespSimpleCacheTest")
public class RespSimpleCacheTest extends SingleNodeRespBaseTest {

   protected CacheMode cacheMode = CacheMode.LOCAL;
   protected boolean simpleCache;

   @Factory
   public Object[] factory() {
      return new Object[] {
            new RespSimpleCacheTest(),
            new RespSimpleCacheTest().simpleCache(),
      };
   }

   protected RespSimpleCacheTest simpleCache() {
      this.cacheMode = CacheMode.LOCAL;
      this.simpleCache = true;
      return this;
   }

   @Override
   protected String parameters() {
      return "[simpleCache=" + simpleCache + ", cacheMode=" + cacheMode + "]";
   }

   @Override
   protected void amendConfiguration(ConfigurationBuilder builder) {
      if (simpleCache) {
         builder.clustering().cacheMode(CacheMode.LOCAL).simpleCache(true);
      } else {
         builder.clustering().cacheMode(cacheMode);
      }
   }

   public void testSetGetDelete() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set("k1", "v1");
      String v = redis.get("k1");
      assertThat(v).isEqualTo("v1");

      redis.del("k1");

      assertThat(redis.get("k1")).isNull();
      assertThat(redis.get("something")).isNull();
   }

   public void testHashStructureOperations() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.hdel("not-existent", "unknown")).isEqualTo(0);
      assertThat(redis.hgetall("not-existent")).isEmpty();

      Map<String, String> map = Map.of("key1", "value1", "key2", "value2", "key3", "value3");
      assertThat(redis.hset("HSET", map)).isEqualTo(3);

      // Updating some keys should return 0.
      assertThat(redis.hset("HSET", Map.of("key1", "other-value1"))).isEqualTo(0);

      // Mixing update and new keys.
      assertThat(redis.hset("HSET", Map.of("key2", "other-value2", "key4", "value4"))).isEqualTo(1);

      assertThat(redis.hget("HSET", "key1")).isEqualTo("other-value1");
      assertThat(redis.hget("HSET", "unknown")).isNull();
      assertThat(redis.hget("UNKNOWN", "unknown")).isNull();
      assertThat(redis.hgetall("HSET"))
            .containsAllEntriesOf(
                  Map.of("key1", "other-value1",
                        "key2", "other-value2",
                        "key3", "value3",
                        "key4", "value4"));

      assertWrongType(() -> redis.set("plain", "string"), () -> redis.hmset("plain", Map.of("k1", "v1")));
      assertWrongType(() -> {}, () -> redis.hget("plain", "k1"));
   }

   public void testSortedSetOperations() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.zcard("not_existing")).isEqualTo(0);
      assertThat(redis.zadd("people", ZAddArgs.Builder.ch(),
            just(18.9, "fabio"),
            just(21.9, "marc")))
            .isEqualTo(2);
      assertThat(redis.zcard("people")).isEqualTo(2);
      assertThat(redis.zrangeWithScores("people", 0, -1))
            .containsExactly(just(18.9, "fabio"), just(21.9, "marc"));
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zcard("another"));
   }

   public void testListOperations() {
      RedisCommands<String, String> redis = redisConnection.sync();

      long result = redis.rpush("people", "tristan");
      assertThat(result).isEqualTo(1);

      result = redis.rpush("people", "william");
      assertThat(result).isEqualTo(2);

      result = redis.rpush("people", "william", "jose", "pedro");
      assertThat(result).isEqualTo(5);
      assertThat(redis.lrange("people", 0, 4)).containsExactly("tristan", "william", "william", "jose", "pedro");

      assertWrongType(() -> redis.set("leads", "tristan"), () ->  redis.rpush("leads", "william"));
   }

   public void testSetOperations() {
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
      assertWrongType(() -> redis.set("leads", "tristan"), () ->  redis.sadd("leads", "william"));
      // SADD on an existing key that contains a List, not a Set!
      // Create a List
      assertWrongType(() -> redis.lpush("listleads", "tristan"), () ->  redis.sadd("listleads", "william"));
   }
}
