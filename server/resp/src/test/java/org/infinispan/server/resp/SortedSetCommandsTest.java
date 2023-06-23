package org.infinispan.server.resp;

import io.lettuce.core.Range;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.sync.RedisCommands;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static io.lettuce.core.Range.Boundary.excluding;
import static io.lettuce.core.Range.Boundary.including;
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

   public void testZCARD() {
      assertThat(redis.zcard("not_existing")).isEqualTo(0);
      assertThat(redis.zadd("people", ZAddArgs.Builder.ch(),
            just(18.9, "fabio"),
            just(21.9, "marc")))
            .isEqualTo(2);
      assertThat(redis.zcard("people")).isEqualTo(2);
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zcard("another"));
   }

   public void testZCOUNT() {
      Range<Double> unbounded = Range.unbounded();
      assertThat(redis.zcount("not_existing", unbounded)).isEqualTo(0);
      redis.zadd("people", ZAddArgs.Builder.ch(),
            ScoredValue.just(-10, "tristan"),
            ScoredValue.just(1, "ryan"),
            ScoredValue.just(17, "vittorio"),
            ScoredValue.just(18.9, "fabio"),
            ScoredValue.just(18.9, "jose"),
            ScoredValue.just(18.9, "katia"),
            ScoredValue.just(21.9, "marc"));
      assertThat(redis.zcard("people")).isEqualTo(7);
      assertThat(redis.zcount("people", unbounded)).isEqualTo(7);
      assertThat(redis.zcount("people", Range.from(including(-10d), including(21.9d)))).isEqualTo(7);
      assertThat(redis.zcount("people", Range.from(including(-11d), including(22.9d)))).isEqualTo(7);
      assertThat(redis.zcount("people", Range.from(including(1d), including(17d)))).isEqualTo(2);
      assertThat(redis.zcount("people", Range.from(including(0d), including(18d)))).isEqualTo(2);
      assertThat(redis.zcount("people", Range.from(including(0d), including(18.9d)))).isEqualTo(5);
      assertThat(redis.zcount("people", Range.from(including(18.9d), including(22d)))).isEqualTo(4);
      assertThat(redis.zcount("people", Range.from(excluding(1d), including(19)))).isEqualTo(4);
      assertThat(redis.zcount("people", Range.from(including(1d), excluding(19)))).isEqualTo(5);
      assertThat(redis.zcount("people", Range.from(including(-10d), excluding(18.9)))).isEqualTo(3);
      assertThat(redis.zcount("people", Range.from(including(-10d), excluding(-10.d)))).isEqualTo(0);
      assertThat(redis.zcount("people", Range.from(excluding(-10d), including(-10.d)))).isEqualTo(0);
      assertThat(redis.zcount("people", Range.from(including(-10d), including(-10.d)))).isEqualTo(1);
      assertThat(redis.zcount("people", Range.from(excluding(-10d), excluding(-10.d)))).isEqualTo(0);
      assertThat(redis.zcount("people", Range.from(including(18.9d), excluding(18.9d)))).isEqualTo(0);
      assertThat(redis.zcount("people", Range.from(excluding(18.9d), excluding(18.9d)))).isEqualTo(0);
      assertThat(redis.zcount("people", Range.from(including(18.9d), including(18.9d)))).isEqualTo(3);
      assertThat(redis.zcount("people", Range.from(excluding(18.9d), including(18.9d)))).isEqualTo(0);

      redis.zadd("manyduplicates", ZAddArgs.Builder.ch(),
            ScoredValue.just(1, "a"),
            ScoredValue.just(1, "b"),
            ScoredValue.just(1, "c"),
            ScoredValue.just(2, "d"),
            ScoredValue.just(2, "e"),
            ScoredValue.just(2, "f"),
            ScoredValue.just(2, "g"),
            ScoredValue.just(2, "h"),
            ScoredValue.just(2, "i"),
            ScoredValue.just(3, "j"),
            ScoredValue.just(3, "k"),
            ScoredValue.just(3, "l"),
            ScoredValue.just(3, "m"),
            ScoredValue.just(3, "n"));

      assertThat(redis.zcount("manyduplicates",
            Range.from(including(1), including(3)))).isEqualTo(14);

      assertThat(redis.zcount("manyduplicates",
            Range.from(excluding(1), excluding(3)))).isEqualTo(6);

      assertThat(redis.zcount("manyduplicates",
            Range.from(including(1), excluding(2)))).isEqualTo(3);

      assertThat(redis.zcount("manyduplicates",
            Range.from(excluding(1), including(2)))).isEqualTo(6);

      assertThat(redis.zcount("manyduplicates",
            Range.from(including(1), including(1)))).isEqualTo(3);

      assertThat(redis.zcount("manyduplicates",
            Range.from(including(2), including(2)))).isEqualTo(6);

      assertThat(redis.zcount("manyduplicates",
            Range.from(including(3), including(3)))).isEqualTo(5);

      assertThat(redis.zcount("manyduplicates",
            Range.from(including(1.5), excluding(2.1)))).isEqualTo(6);

      assertThat(redis.zcount("manyduplicates",
            Range.from(excluding(1), excluding(2)))).isEqualTo(0);

      assertThat(redis.zcount("manyduplicates",
            Range.from(excluding(2.5), excluding(3)))).isEqualTo(0);

      assertThat(redis.zcount("manyduplicates",
            Range.from(including(1), excluding(3)))).isEqualTo(9);
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zcount("another", unbounded));
   }

}
