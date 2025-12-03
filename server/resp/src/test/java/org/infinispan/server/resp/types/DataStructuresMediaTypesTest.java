package org.infinispan.server.resp.types;

import static io.lettuce.core.ScoredValue.just;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.resp.SingleNodeRespBaseTest;
import org.testng.annotations.Test;

import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "server.resp.types.DataStructuresMediaTypesTest")
public class DataStructuresMediaTypesTest extends SingleNodeRespBaseTest {

   private boolean simpleCache;
   private MediaType valueType;

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

   public void testRPUSH() {
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

   public void testZADD() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.zadd("people", 10.4, "william")).isEqualTo(1);
      assertThat(redis.zrangeWithScores("people", 0, -1))
            .containsExactly(just(10.4, "william"));
      assertThat(redis.zadd("people", just(13.4, "tristan"))).isEqualTo(1);
      assertThat(redis.zrangeWithScores("people", 0, -1))
            .containsExactly(just(10.4, "william"), just(13.4, "tristan"));
      assertThat(redis.zadd("people", just(13.4, "jose"))).isEqualTo(1);
      assertThat(redis.zrangeWithScores("people", 0, -1))
            .containsExactly(just(10.4, "william"), just(13.4, "jose"), just(13.4, "tristan"));

      assertThat(redis.zadd("people", just(13.4, "xavier"))).isEqualTo(1);
      assertThat(redis.zrangeWithScores("people", 0, -1))
            .containsExactly(just(10.4, "william"), just(13.4, "jose"), just(13.4, "tristan"),  just(13.4, "xavier"));

      // count changes too
      assertThat(redis.zadd("people", ZAddArgs.Builder.ch(),
            just(18.9, "fabio"),
            just(21.9, "marc")))
            .isEqualTo(2);
      assertThat(redis.zrangeWithScores("people", 0, -1))
            .containsExactly(just(10.4, "william"), just(13.4, "jose"),
                  just(13.4, "tristan"),  just(13.4, "xavier"),
                  just(18.9, "fabio"), just(21.9, "marc"));

      // Adds only
      assertThat(redis.zadd("people", ZAddArgs.Builder.nx(),
            just(0.8, "fabio"),
            just(0.9, "xavier"),
            just(1.0, "ryan")))
            .isEqualTo(1);

      assertThat(redis.zrangeWithScores("people", 0, -1))
            .containsExactly(just(1.0, "ryan"), just(10.4, "william"), just(13.4, "jose"),
                  just(13.4, "tristan"),  just(13.4, "xavier"),
                  just(18.9, "fabio"), just(21.9, "marc"));

      // Updates only
      assertThat(redis.zadd("people", ZAddArgs.Builder.xx(),
            just(0.8, "fabio"),
            just(0.9, "xavier"),
            just(1.0, "katia")))
            .isEqualTo(0);

      assertThat(redis.zrangeWithScores("people", 0, -1))
            .containsExactly(just(0.8, "fabio"), just(0.9, "xavier"), just(1.0, "ryan"),
                  just(10.4, "william"), just(13.4, "jose"),
                  just(13.4, "tristan"), just(21.9, "marc"));

      // Updates greater scores and add new values
      assertThat(redis.zadd("people", ZAddArgs.Builder.gt(),
            just(13, "fabio"), // changes to 13 because 13 (new) is greater than 0.8 (current)
            just(0.5, "xavier"), // stays 0.9 because 0.5 (new) is less than 0.9 (current)
            just(2, "katia"))) // added
            .isEqualTo(1);

      assertThat(redis.zrangeWithScores("people", 0, -1))
            .containsExactly(just(0.9, "xavier"), just(1.0, "ryan"),  just(2, "katia"),
                  just(10.4, "william"), just(13, "fabio"), just(13.4, "jose"),
                  just(13.4, "tristan"), just(21.9, "marc"));

      // Updates less than scores and add new values
      assertThat(redis.zadd("people", ZAddArgs.Builder.lt(),
            just(100, "fabio"), // stays 13 because 100 (new) is greater than 13 (current)
            just(0.3, "xavier"), // changes to 0.3 because 0.3 (new) is less than 0.5 (current)
            just(0.2, "vittorio"))) // added
            .isEqualTo(1);

      assertThat(redis.zrangeWithScores("people", 0, -1))
            .containsExactly(just(0.2, "vittorio"), just(0.3, "xavier"),
                  just(1.0, "ryan"),  just(2, "katia"),
                  just(10.4, "william"), just(13, "fabio"), just(13.4, "jose"),
                  just(13.4, "tristan"), just(21.9, "marc"));
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zadd("another", 2.3, "tristan"));
   }

   @SuppressWarnings("unchecked")
   public void testZaddInfinityScores() {
      RedisCommands<String, String> redis = redisConnection.sync();

      String key = k();
      String m0 = v(0);
      String m1 = v(1);
      String m2 = v(2);

      assertThat(redis.zadd(key,
            just(Double.POSITIVE_INFINITY, m0),
            just(Double.NEGATIVE_INFINITY, m1),
            just(1, m2))
      ).isEqualTo(3);

      // Allows increment with +inf
      assertThat(redis.zaddincr(key, Double.POSITIVE_INFINITY, m2)).isEqualTo(Double.POSITIVE_INFINITY);

      // LT/GT validation on other members should return null.
      assertThat(redis.zaddincr(key, ZAddArgs.Builder.lt(), 1, m0)).isNull();
      assertThat(redis.zaddincr(key, ZAddArgs.Builder.lt(), 1, m1)).isNull();

      // Adding +inf with -inf is NaN.
      assertThatThrownBy(() -> redis.zaddincr(key, Double.NEGATIVE_INFINITY, m2))
            .hasMessageContaining("resulting score is not a number (NaN)");

      // Adding +inf with 42 is +inf.
      assertThat(redis.zaddincr(key, 42, m0)).isInfinite();
   }

   @Override
   protected void amendConfiguration(ConfigurationBuilder configurationBuilder) {
      if (simpleCache) {
         configurationBuilder.clustering().cacheMode(CacheMode.LOCAL).simpleCache(true);
      } else {
         configurationBuilder.clustering().cacheMode(cacheMode);
      }
      configurationBuilder.encoding().value().mediaType(valueType.toString());
   }

   private DataStructuresMediaTypesTest withValueType(MediaType type) {
      this.valueType = type;
      return this;
   }

   private DataStructuresMediaTypesTest withSimpleCache() {
      this.simpleCache = true;
      return this;
   }

   private DataStructuresMediaTypesTest withCacheMode(CacheMode mode) {
      this.cacheMode = mode;
      return this;
   }

   @Override
   public Object[] factory() {
      List<DataStructuresMediaTypesTest> instances = new ArrayList<>();
      MediaType[] types = new MediaType[] {
            MediaType.APPLICATION_OCTET_STREAM,
            MediaType.APPLICATION_PROTOSTREAM,
            MediaType.APPLICATION_OBJECT,
            MediaType.TEXT_PLAIN,
      };
      for (MediaType value : types) {
         instances.add(new DataStructuresMediaTypesTest().withValueType(value).withCacheMode(CacheMode.LOCAL));
         instances.add(new DataStructuresMediaTypesTest().withValueType(value).withSimpleCache());
         instances.add(new DataStructuresMediaTypesTest().withValueType(value).withCacheMode(CacheMode.LOCAL).withAuthorization());
         instances.add(new DataStructuresMediaTypesTest().withValueType(value).withSimpleCache().withAuthorization());
      }
      return instances.toArray();
   }

   @Override
   protected String parameters() {
      return "[simpleCache=" + simpleCache + ", cacheMode=" + cacheMode + ", value=" + valueType + "]";
   }
}
