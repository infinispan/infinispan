package org.infinispan.server.resp;

import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.sync.RedisCommands;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static io.lettuce.core.ScoredValue.just;
import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;

/**
 * RESP Sorted set commands testing
 *
 * @since 15.0
 */
@Test(groups = "functional", testName = "server.resp.SortedSetCommandsTest")
public class SortedSetCommandsTest extends SingleNodeRespBaseTest {

   RedisCommands<String, String> redis;

   @BeforeMethod
   public void initConnection() {
      redis = redisConnection.sync();
   }

   public void testZADD() {
      assertThat(redis.zadd("people", 10.4, "william")).isEqualTo(1);
      assertThat(redis.zrangeWithScores("people", 1, -1))
            .containsExactly(just(10.4, "william"));
      assertThat(redis.zadd("people", just(13.4, "tristan"))).isEqualTo(1);
      assertThat(redis.zrangeWithScores("people", 1, -1))
            .containsExactly(just(10.4, "william"), just(13.4, "tristan"));
      assertThat(redis.zadd("people", just(13.4, "jose"))).isEqualTo(1);
      assertThat(redis.zrangeWithScores("people", 1, -1))
            .containsExactly(just(10.4, "william"), just(13.4, "jose"), just(13.4, "tristan"));

      assertThat(redis.zadd("people", just(13.4, "xavier"))).isEqualTo(1);
      assertThat(redis.zrangeWithScores("people", 1, -1))
            .containsExactly(just(10.4, "william"), just(13.4, "jose"), just(13.4, "tristan"),  just(13.4, "xavier"));

      // count changes too
      assertThat(redis.zadd("people", ZAddArgs.Builder.ch(),
            just(18.9, "fabio"),
            just(21.9, "marc")))
            .isEqualTo(2);
      assertThat(redis.zrangeWithScores("people", 1, -1))
            .containsExactly(just(10.4, "william"), just(13.4, "jose"),
                  just(13.4, "tristan"),  just(13.4, "xavier"),
                  just(18.9, "fabio"), just(21.9, "marc"));

      // Adds only
      assertThat(redis.zadd("people", ZAddArgs.Builder.nx(),
            just(0.8, "fabio"),
            just(0.9, "xavier"),
            just(1.0, "ryan")))
      .isEqualTo(1);

      assertThat(redis.zrangeWithScores("people", 1, -1))
            .containsExactly(just(1.0, "ryan"), just(10.4, "william"), just(13.4, "jose"),
                  just(13.4, "tristan"),  just(13.4, "xavier"),
                  just(18.9, "fabio"), just(21.9, "marc"));

      // Updates only
      assertThat(redis.zadd("people", ZAddArgs.Builder.xx(),
            just(0.8, "fabio"),
            just(0.9, "xavier"),
            just(1.0, "katia")))
            .isEqualTo(0);

      assertThat(redis.zrangeWithScores("people", 1, -1))
            .containsExactly(just(0.8, "fabio"), just(0.9, "xavier"), just(1.0, "ryan"),
                  just(10.4, "william"), just(13.4, "jose"),
                  just(13.4, "tristan"), just(21.9, "marc"));

      // Updates greater scores and add new values
      assertThat(redis.zadd("people", ZAddArgs.Builder.gt(),
            just(13, "fabio"), // changes to 13 because 13 (new) is greater than 0.8 (current)
            just(0.5, "xavier"), // stays 0.9 because 0.5 (new) is less than 0.9 (current)
            just(2, "katia"))) // added
            .isEqualTo(1);

      assertThat(redis.zrangeWithScores("people", 1, -1))
            .containsExactly(just(0.9, "xavier"), just(1.0, "ryan"),  just(2, "katia"),
                  just(10.4, "william"), just(13, "fabio"), just(13.4, "jose"),
                  just(13.4, "tristan"), just(21.9, "marc"));

      // Updates less than scores and add new values
      assertThat(redis.zadd("people", ZAddArgs.Builder.lt(),
            just(100, "fabio"), // stays 13 because 100 (new) is greater than 13 (current)
            just(0.3, "xavier"), // changes to 0.3 because 0.3 (new) is less than 0.5 (current)
            just(0.2, "vittorio"))) // added
            .isEqualTo(1);

      assertThat(redis.zrangeWithScores("people", 1, -1))
            .containsExactly(just(0.2, "vittorio"), just(0.3, "xavier"),
                  just(1.0, "ryan"),  just(2, "katia"),
                  just(10.4, "william"), just(13, "fabio"), just(13.4, "jose"),
                  just(13.4, "tristan"), just(21.9, "marc"));

      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zadd("another", 2.3, "tristan"));
   }

}
