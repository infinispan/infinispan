package org.infinispan.server.resp;

import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.ZAggregateArgs;
import io.lettuce.core.ZStoreArgs;
import io.lettuce.core.api.sync.RedisCommands;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static io.lettuce.core.Range.Boundary.excluding;
import static io.lettuce.core.Range.Boundary.including;
import static io.lettuce.core.Range.Boundary.unbounded;
import static io.lettuce.core.Range.create;
import static io.lettuce.core.Range.from;
import static io.lettuce.core.ScoredValue.just;
import static io.lettuce.core.ZAggregateArgs.Builder.max;
import static io.lettuce.core.ZAggregateArgs.Builder.min;
import static io.lettuce.core.ZAggregateArgs.Builder.sum;
import static io.lettuce.core.ZAggregateArgs.Builder.weights;
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

   public void testZADDINCR() {
      //ZADD people INCR 30 tristan
      assertThat(redis.zaddincr("people",  30, "tristan")).isEqualTo(30);
      //ZRANGE people 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("people", 0, -1)).containsExactly(just(30, "tristan"));
      //ZADD people INCR 2 tristan
      assertThat(redis.zaddincr("people",  2, "tristan")).isEqualTo(32);
      //ZRANGE people 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("people", 0, -1)).containsExactly(just(32, "tristan"));
      //ZADD people INCR -4 tristan
      assertThat(redis.zaddincr("people",  -4, "tristan")).isEqualTo(28);
      //ZRANGE people 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("people", 0, -1)).containsExactly(just(28, "tristan"));
      //ZADD people NX INCR -4 tristan
      assertThat(redis.zaddincr("people", ZAddArgs.Builder.nx(), -4,"tristan")).isNull();
      //ZADD people XX INCR -4 jose
      assertThat(redis.zaddincr("people", ZAddArgs.Builder.xx(), -4,"jose")).isNull();
      //ZRANGE people 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("people", 0, -1)).containsExactly(just(28, "tristan"));
      //ZADD people NX INCR -4 jose
      assertThat(redis.zaddincr("people", ZAddArgs.Builder.nx(), -4, "jose")).isEqualTo(-4);
      //ZADD people XX INCR -4 tristan
      assertThat(redis.zaddincr("people", ZAddArgs.Builder.xx(), -4, "tristan")).isEqualTo(24);
      //ZRANGE people 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("people", 0, -1)).containsExactly(just(-4, "jose"), just(24, "tristan"));
      //ZADD people LT INCR -4 tristan
      assertThat(redis.zaddincr("people", ZAddArgs.Builder.lt(), -4, "tristan")).isEqualTo(20);
      //ZRANGE people 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("people", 0, -1)).containsExactly(just(-4, "jose"), just(20, "tristan"));
      //ZADD people GT INCR -4 tristan
      assertThat(redis.zaddincr("people", ZAddArgs.Builder.gt(), -4, "tristan")).isNull();
      //ZRANGE people 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("people", 0, -1)).containsExactly(just(-4, "jose"), just(20, "tristan"));
      //ZADD people LT INCR -4 tristan
      assertThat(redis.zaddincr("people", ZAddArgs.Builder.lt(), 4, "tristan")).isNull();
      //ZRANGE people 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("people", 0, -1)).containsExactly(just(-4, "jose"), just(20, "tristan"));
      //ZADD people GT INCR 4 tristan
      assertThat(redis.zaddincr("people", ZAddArgs.Builder.gt(), 4, "tristan")).isEqualTo(24);
      //ZRANGE people 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("people", 0, -1)).containsExactly(just(-4, "jose"), just(24, "tristan"));
      //ZADD people LT INCR 2 vittorio
      assertThat(redis.zaddincr("people", ZAddArgs.Builder.lt(), 2, "vittorio")).isEqualTo(2);
      //ZRANGE people 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("people", 0, -1)).containsExactly(just(-4, "jose"), just(2, "vittorio"), just(24, "tristan"));
      //ZADD people GT INCR -10 pedro
      assertThat(redis.zaddincr("people", ZAddArgs.Builder.gt(), -10, "pedro")).isEqualTo(-10);
      //ZRANGE people 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("people", 0, -1)).containsExactly(
            just(-10, "pedro"),
            just(-4, "jose"),
            just(2, "vittorio"),
            just(24, "tristan"));
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zaddincr("another", 2.3, "tristan"));
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
            just(-10, "tristan"),
            just(1, "ryan"),
            just(17, "vittorio"),
            just(18.9, "fabio"),
            just(18.9, "jose"),
            just(18.9, "katia"),
            just(21.9, "marc"));
      assertThat(redis.zcard("people")).isEqualTo(7);
      assertThat(redis.zcount("people", unbounded)).isEqualTo(7);
      assertThat(redis.zcount("people", from(including(-10d), including(21.9d)))).isEqualTo(7);
      assertThat(redis.zcount("people", from(including(-11d), including(22.9d)))).isEqualTo(7);
      assertThat(redis.zcount("people", from(including(1d), including(17d)))).isEqualTo(2);
      assertThat(redis.zcount("people", from(including(0d), including(18d)))).isEqualTo(2);
      assertThat(redis.zcount("people", from(including(0d), including(18.9d)))).isEqualTo(5);
      assertThat(redis.zcount("people", from(including(18.9d), including(22d)))).isEqualTo(4);
      assertThat(redis.zcount("people", from(excluding(1d), including(19)))).isEqualTo(4);
      assertThat(redis.zcount("people", from(including(1d), excluding(19)))).isEqualTo(5);
      assertThat(redis.zcount("people", from(including(-10d), excluding(18.9)))).isEqualTo(3);
      assertThat(redis.zcount("people", from(including(-10d), excluding(-10.d)))).isEqualTo(0);
      assertThat(redis.zcount("people", from(excluding(-10d), including(-10.d)))).isEqualTo(0);
      assertThat(redis.zcount("people", from(including(-10d), including(-10.d)))).isEqualTo(1);
      assertThat(redis.zcount("people", from(excluding(-10d), excluding(-10.d)))).isEqualTo(0);
      assertThat(redis.zcount("people", from(including(18.9d), excluding(18.9d)))).isEqualTo(0);
      assertThat(redis.zcount("people", from(excluding(18.9d), excluding(18.9d)))).isEqualTo(0);
      assertThat(redis.zcount("people", from(including(18.9d), including(18.9d)))).isEqualTo(3);
      assertThat(redis.zcount("people", from(excluding(18.9d), including(18.9d)))).isEqualTo(0);

      redis.zadd("manyduplicates", ZAddArgs.Builder.ch(),
            just(1, "a"),
            just(1, "b"),
            just(1, "c"),
            just(2, "d"),
            just(2, "e"),
            just(2, "f"),
            just(2, "g"),
            just(2, "h"),
            just(2, "i"),
            just(3, "j"),
            just(3, "k"),
            just(3, "l"),
            just(3, "m"),
            just(3, "n"));

      assertThat(redis.zcount("manyduplicates",
            from(including(1), including(3)))).isEqualTo(14);

      assertThat(redis.zcount("manyduplicates",
            from(excluding(1), excluding(3)))).isEqualTo(6);

      assertThat(redis.zcount("manyduplicates",
            from(including(1), excluding(2)))).isEqualTo(3);

      assertThat(redis.zcount("manyduplicates",
            from(excluding(1), including(2)))).isEqualTo(6);

      assertThat(redis.zcount("manyduplicates",
            from(including(1), including(1)))).isEqualTo(3);

      assertThat(redis.zcount("manyduplicates",
            from(including(2), including(2)))).isEqualTo(6);

      assertThat(redis.zcount("manyduplicates",
            from(including(3), including(3)))).isEqualTo(5);

      assertThat(redis.zcount("manyduplicates",
            from(including(1.5), excluding(2.1)))).isEqualTo(6);

      assertThat(redis.zcount("manyduplicates",
            from(excluding(1), excluding(2)))).isEqualTo(0);

      assertThat(redis.zcount("manyduplicates",
            from(excluding(2.5), excluding(3)))).isEqualTo(0);

      assertThat(redis.zcount("manyduplicates",
            from(including(1), excluding(3)))).isEqualTo(9);
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zcount("another", unbounded));
   }

   public void testZPOPMIN() {
      assertThat(redis.zpopmin("not_existing").isEmpty()).isTrue();
      assertThat(redis.zpopmin("not_existing", 2)).isEmpty();
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(-10, "tristan"),
            just(1, "ryan"),
            just(17, "vittorio"),
            just(18.9, "fabio"),
            just(18.9, "jose"),
            just(18.9, "katia"),
            just(21.9, "marc"));
      assertThat(redis.zcard("people")).isEqualTo(7);
      assertThat(redis.zpopmin("people")).isEqualTo(just(-10.0, "tristan"));
      assertThat(redis.zcard("people")).isEqualTo(6);
      assertThat(redis.zpopmin("people", 2))
            .containsExactly(just(1, "ryan"), just(17, "vittorio"));
      assertThat(redis.zcard("people")).isEqualTo(4);
      redis.zpopmin("people", 10);
      assertThat(redis.exists("people")).isEqualTo(0);
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zpopmin("another"));
   }

   public void testZPOPMAX() {
      assertThat(redis.zpopmax("not_existing").isEmpty()).isTrue();
      assertThat(redis.zpopmax("not_existing", 2)).isEmpty();
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(-10, "tristan"),
            just(1, "ryan"),
            just(17, "vittorio"),
            just(18.9, "fabio"),
            just(18.9, "jose"),
            just(18.9, "katia"),
            just(21.9, "marc"));
      assertThat(redis.zcard("people")).isEqualTo(7);
      assertThat(redis.zpopmax("people")).isEqualTo(just(21.9, "marc"));
      assertThat(redis.zcard("people")).isEqualTo(6);
      assertThat(redis.zpopmax("people", 2))
            .containsExactly(just(18.9, "katia"), just(18.9, "jose"));
      assertThat(redis.zcard("people")).isEqualTo(4);
      redis.zpopmax("people", 10);
      assertThat(redis.exists("people")).isEqualTo(0);
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zpopmax("another"));
   }

   public void testZSCORE() {
      assertThat(redis.zscore("not_existing", "no_existing")).isNull();
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(-10, "tristan"),
            just(1, "ryan"),
            just(17, "vittorio"),
            just(18.9, "fabio"),
            just(18.9, "jose"),
            just(18.9, "katia"),
            just(21.9, "marc"));
      assertThat(redis.zscore("people", "jose")).isEqualTo(18.9);
      assertThat(redis.zscore("people", "takolo")).isNull();
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zscore("another", "tristan"));
   }

   public void testZRANGE() {
      // ZRANGE people 0 -1 REV
      assertThat(redis.zrange("people", 0, -1)).isEmpty();
      // ZADD people -10 tristan 1 ryan 17 vittorio 18.9 fabio 18.9 jose 18.9 katia 21.9 marc
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(-10, "tristan"), just(1, "ryan"), just(17, "vittorio"),
            just(18.9, "fabio"), just(18.9, "jose"), just(18.9, "katia"),
            just(21.9, "marc"));
      // ZRANGE people 0 -1
      assertThat(redis.zrange("people", 0, -1))
            .containsExactly("tristan", "ryan", "vittorio", "fabio", "jose", "katia", "marc");
      // ZRANGE people -1 -1
      assertThat(redis.zrange("people", -1, -1)).containsExactly("marc");
      // ZRANGE people -3 -3
      assertThat(redis.zrange("people", -3, -3)).containsExactly("jose");
      // ZRANGE people 0 0
      assertThat(redis.zrange("people", 0, 0)).containsExactly("tristan");
      // ZRANGE people 3 3
      assertThat(redis.zrange("people", 3, 3)).containsExactly("fabio");
      // ZRANGE people 0 100
      assertThat(redis.zrange("people", 0, 100))
            .containsExactly("tristan", "ryan", "vittorio", "fabio", "jose", "katia", "marc");
      // ZRANGE people 2 5
      assertThat(redis.zrange("people", 2, 5))
            .containsExactly("vittorio", "fabio", "jose", "katia");
      // ZRANGE people -1 0
      assertThat(redis.zrange("people", -1, 0)).isEmpty();
      // ZRANGE people 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("people", 0, -1))
            .containsExactly(just(-10, "tristan"), just(1, "ryan"), just(17, "vittorio"),
                  just(18.9, "fabio"), just(18.9, "jose"), just(18.9, "katia"),
                  just(21.9, "marc"));

      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zrange("another", 0, -1));
   }

   public void testZREVRANGE() {
      // ZRANGE people 0 -1 REV
      assertThat(redis.zrevrange("people", 0, -1)).isEmpty();
      // ZADD people -10 tristan 1 ryan 17 vittorio 18.9 fabio 18.9 jose 18.9 katia 21.9 marc
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(-10, "tristan"), just(1, "ryan"), just(17, "vittorio"),
            just(18.9, "fabio"), just(18.9, "jose"), just(18.9, "katia"),
            just(21.9, "marc"));
      // ZRANGE people 0 -1 REV
      assertThat(redis.zrevrange("people", 0, -1))
            .containsExactly("marc", "katia", "jose", "fabio", "vittorio", "ryan", "tristan");
      // ZRANGE people 0 0 REV
      assertThat(redis.zrevrange("people", 0, 0)).containsExactly("marc");
      // ZRANGE people -1 -1 REV
      assertThat(redis.zrevrange("people", -1, -1)).containsExactly("tristan");
      // ZRANGE people -3 -3 REV
      assertThat(redis.zrevrange("people", -3, -3)).containsExactly("vittorio");
      // ZRANGE people 0 0 REV
      assertThat(redis.zrevrange("people", 0, 0)).containsExactly("marc");
      // ZRANGE people 3 3 REV
      assertThat(redis.zrevrange("people", 3, 3)).containsExactly("fabio");
      // ZRANGE people 0 100 REV
      assertThat(redis.zrevrange("people", 0, 100))
            .containsExactly("marc", "katia", "jose", "fabio", "vittorio", "ryan", "tristan");
      // ZRANGE people 2 5 REV
      assertThat(redis.zrevrange("people", 2, 5))
            .containsExactly("jose", "fabio", "vittorio", "ryan");
      // ZRANGE people -1 0 REV
      assertThat(redis.zrevrange("people", -1, 0)).isEmpty();
      // ZRANGE people 0 -1 REV WITHSCORES
      assertThat(redis.zrevrangeWithScores("people", 0, -1))
            .containsExactly(just(21.9, "marc"), just(18.9, "katia"), just(18.9, "jose"),
                  just(18.9, "fabio"), just(17, "vittorio"), just(1, "ryan"),
                  just(-10, "tristan"));

      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zrevrange("another", 0, -1));
   }

   public void testZRANGEbyScore() {
      //ZRANGE people -inf +inf BYSCORE WITHSCORES
      assertThat(redis.zrangebyscore("people", Range.unbounded())).isEmpty();
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(-10, "joselie"),
            just(-10, "tristan"),
            just(1, "dan"),
            just(1, "gustavo"),
            just(1, "ryan"),
            just(17, "vittorio"),
            just(18, "adrian"),
            just(18, "audrey"),
            just(18, "emmanuel"),
            just(18, "fabio"),
            just(18, "jose"),
            just(18, "katia"),
            just(18, "zineb"),
            just(21.9, "anna"));
      //ZRANGE people -inf +inf BYSCORE
      assertThat(redis.zrangebyscore("people", Range.unbounded()))
            .containsExactly("joselie", "tristan", "dan", "gustavo", "ryan",
                  "vittorio", "adrian", "audrey", "emmanuel", "fabio", "jose", "katia", "zineb", "anna");
      //ZRANGE people -inf +inf BYSCORE WITHSCORES
      assertThat(redis.zrangebyscoreWithScores("people", Range.unbounded()))
            .containsExactly(just(-10, "joselie"),
                  just(-10, "tristan"),
                  just(1, "dan"),
                  just(1, "gustavo"),
                  just(1, "ryan"),
                  just(17, "vittorio"),
                  just(18, "adrian"),
                  just(18, "audrey"),
                  just(18, "emmanuel"),
                  just(18, "fabio"),
                  just(18, "jose"),
                  just(18, "katia"),
                  just(18, "zineb"),
                  just(21.9, "anna"));
      //ZRANGE people -inf +inf BYSCORE WITHSCORES LIMIT 3 7
      assertThat(redis.zrangebyscoreWithScores("people", Range.unbounded(), Limit.create(3, 7)))
            .containsExactly(just(1, "gustavo"), just(1, "ryan"),
                  just(17, "vittorio"), just(18, "adrian"), just(18, "audrey"),
                  just(18, "emmanuel"), just(18, "fabio"));
      //ZRANGE people -inf 18 BYSCORE WITHSCORES
      assertThat(redis.zrangebyscoreWithScores("people", from(unbounded(), including(18))))
            .containsExactly(just(-10, "joselie"),
                  just(-10, "tristan"),
                  just(1, "dan"),
                  just(1, "gustavo"),
                  just(1, "ryan"),
                  just(17, "vittorio"),
                  just(18, "adrian"),
                  just(18, "audrey"),
                  just(18, "emmanuel"),
                  just(18, "fabio"),
                  just(18, "jose"),
                  just(18, "katia"),
                  just(18, "zineb")
            );
      //ZRANGE people -inf (18 BYSCORE WITHSCORES
      assertThat(redis.zrangebyscoreWithScores("people", from(unbounded(), excluding(18))))
            .containsExactly(just(-10, "joselie"),
                  just(-10, "tristan"),
                  just(1, "dan"),
                  just(1, "gustavo"),
                  just(1, "ryan"),
                  just(17, "vittorio"));
      //ZRANGE people 18 +inf BYSCORE WITHSCORES
      assertThat(redis.zrangebyscoreWithScores("people", from(including(18), unbounded())))
            .containsExactly(just(18, "adrian"),
                  just(18, "audrey"),
                  just(18, "emmanuel"),
                  just(18, "fabio"),
                  just(18, "jose"),
                  just(18, "katia"),
                  just(18, "zineb"),
                  just(21.9, "anna"));
      //ZRANGE people (18 +inf BYSCORE WITHSCORES
      assertThat(redis.zrangebyscoreWithScores("people", from(excluding(18), unbounded())))
            .containsExactly(just(21.9, "anna"));
      //ZRANGE people 18 18 BYSCORE WITHSCORES
      assertThat(redis.zrangebyscoreWithScores("people", from(including(18), including(18))))
            .containsExactly(just(18, "adrian"),
                  just(18, "audrey"),
                  just(18, "emmanuel"),
                  just(18, "fabio"),
                  just(18, "jose"),
                  just(18, "katia"),
                  just(18, "zineb"));
      //ZRANGE people (18 18 BYSCORE WITHSCORES
      assertThat(redis.zrangebyscoreWithScores("people", from(excluding(18), including(18)))).isEmpty();
      //ZRANGE people 18 (18 BYSCORE WITHSCORES
      assertThat(redis.zrangebyscoreWithScores("people", from(including(18), excluding(18)))).isEmpty();
      //ZRANGE people (18 (18 BYSCORE WITHSCORES
      assertThat(redis.zrangebyscoreWithScores("people", from(excluding(18), excluding(18)))).isEmpty();
      //ZRANGE people -11 17.5 BYSCORE WITHSCORES
      assertThat(redis.zrangebyscoreWithScores("people", from(including(-11), including(17.5))))
            .containsExactly(just(-10, "joselie"),
                  just(-10, "tristan"),
                  just(1, "dan"),
                  just(1, "gustavo"),
                  just(1, "ryan"),
                  just(17, "vittorio")
            );
      //ZRANGE people -10 1 BYSCORE WITHSCORES
      assertThat(redis.zrangebyscoreWithScores("people", from(including(-10), including(1))))
            .containsExactly(
                  just(-10, "joselie"),
                  just(-10, "tristan"),
                  just(1, "dan"),
                  just(1, "gustavo"),
                  just(1, "ryan")
            );
      //ZRANGE people (-10 17 BYSCORE WITHSCORES
      assertThat(redis.zrangebyscoreWithScores("people", from(excluding(-10), including(17))))
            .containsExactly(
                  just(1, "dan"),
                  just(1, "gustavo"),
                  just(1, "ryan"),
                  just(17, "vittorio")
            );
      //ZRANGE people (-10 (17 BYSCORE WITHSCORES
      assertThat(redis.zrangebyscoreWithScores("people", from(excluding(-10), excluding(17))))
            .containsExactly(
                  just(1, "dan"),
                  just(1, "gustavo"),
                  just(1, "ryan")
            );
      //ZRANGE people (-10 (17 BYSCORE WITHSCORES LIMIT 1 1
      assertThat(redis.zrevrangebyscoreWithScores("people", from(excluding(-10), excluding(17)), Limit.create(1, 1)))
            .containsExactly(
                  just(1, "gustavo")
            );
      //ZRANGE people (-10 (17 BYSCORE WITHSCORES LIMIT 1 0
      assertThat(redis.zrangebyscoreWithScores("people", from(excluding(-10), excluding(17)), Limit.create(1, 0)))
            .isEmpty();
      //ZRANGE people (-10 (17 BYSCORE WITHSCORES LIMIT 1 -1
      assertThat(redis.zrangebyscoreWithScores("people", from(excluding(-10), excluding(17)), Limit.create(1, -1)))
            .containsExactly(
                  just(1, "gustavo"),
                  just(1, "ryan"));
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zrangebyscore("another", Range.<Double>unbounded()));
   }

   public void testZREVRANGEbyScore() {
      // ZRANGE people +inf -inf BYSCORE WITHSCORES
      assertThat(redis.zrevrangebyscore("people", Range.unbounded())).isEmpty();
      // ZADD people -10 joselie -10 tristan 1 dan 1 gustavo 1 ryan 17 vittorio 18 adrian 18 audrey 18 emmanuel 18 fabio 18 jose 18 katia 18 zineb 18 anna
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(-10, "joselie"),
            just(-10, "tristan"),
            just(1, "dan"),
            just(1, "gustavo"),
            just(1, "ryan"),
            just(17, "vittorio"),
            just(18, "adrian"),
            just(18, "audrey"),
            just(18, "emmanuel"),
            just(18, "fabio"),
            just(18, "jose"),
            just(18, "katia"),
            just(18, "zineb"),
            just(21.9, "anna"));
      // ZRANGE people +inf -inf BYSCORE REV
      assertThat(redis.zrevrangebyscore("people", Range.unbounded()))
            .containsExactly("anna", "zineb", "katia", "jose",
            "fabio", "emmanuel", "audrey", "adrian", "vittorio", "ryan", "gustavo", "dan", "tristan", "joselie");
      // ZRANGE people +inf -inf BYSCORE REV WITHSCORES
      assertThat(redis.zrevrangebyscoreWithScores("people", Range.unbounded()))
            .containsExactly(
                  just(21.9, "anna"),
                  just(18, "zineb"),
                  just(18, "katia"),
                  just(18, "jose"),
                  just(18, "fabio"),
                  just(18, "emmanuel"),
                  just(18, "audrey"),
                  just(18, "adrian"),
                  just(17, "vittorio"),
                  just(1, "ryan"),
                  just(1, "gustavo"),
                  just(1, "dan"),
                  just(-10, "tristan"),
                  just(-10, "joselie")
            );
      // ZRANGE people +inf -inf BYSCORE REV WITHSCORES LIMIT 3 7
      assertThat(redis.zrevrangebyscoreWithScores("people", Range.unbounded(), Limit.create(3, 7)))
            .containsExactly(
                  just(18, "jose"),
                  just(18, "fabio"),
                  just(18, "emmanuel"),
                  just(18, "audrey"),
                  just(18, "adrian"),
                  just(17, "vittorio"),
                  just(1, "ryan"));

      // ZRANGE people (18 -inf BYSCORE REV WITHSCORES
      assertThat(redis.zrevrangebyscoreWithScores("people", from(unbounded(), excluding(18))))
            .containsExactly(
                  just(17, "vittorio"),
                  just(1, "ryan"),
                  just(1, "gustavo"),
                  just(1, "dan"),
                  just(-10, "tristan"),
                  just(-10, "joselie")
            );
      // ZRANGE people 18 -inf BYSCORE REV WITHSCORES
      assertThat(redis.zrevrangebyscoreWithScores("people", from(unbounded(), including(18))))
            .containsExactly(
                  just(18, "zineb"),
                  just(18, "katia"),
                  just(18, "jose"),
                  just(18, "fabio"),
                  just(18, "emmanuel"),
                  just(18, "audrey"),
                  just(18, "adrian"),
                  just(17, "vittorio"),
                  just(1, "ryan"),
                  just(1, "gustavo"),
                  just(1, "dan"),
                  just(-10, "tristan"),
                  just(-10, "joselie")
            );
      // ZRANGE people +inf (18 BYSCORE REV WITHSCORES
      assertThat(redis.zrevrangebyscoreWithScores("people", from(excluding(18), unbounded())))
            .containsExactly(just(21.9, "anna"));
      // ZRANGE people +inf 18 BYSCORE REV WITHSCORES
      assertThat(redis.zrevrangebyscoreWithScores("people", from(including(18), unbounded())))
            .containsExactly(
                  just(21.9, "anna"),
                  just(18, "zineb"),
                  just(18, "katia"),
                  just(18, "jose"),
                  just(18, "fabio"),
                  just(18, "emmanuel"),
                  just(18, "audrey"),
                  just(18, "adrian")
            );
      // ZRANGE people 18 18 BYSCORE REV WITHSCORES
      assertThat(redis.zrevrangebyscoreWithScores("people", from(including(18), including(18))))
            .containsExactly(
                  just(18, "zineb"),
                  just(18, "katia"),
                  just(18, "jose"),
                  just(18, "fabio"),
                  just(18, "emmanuel"),
                  just(18, "audrey"),
                  just(18, "adrian")
            );
      // ZRANGE people 18 (18 BYSCORE REV WITHSCORES
      assertThat(redis.zrevrangebyscoreWithScores("people", from(excluding(18), including(18)))).isEmpty();
      // ZRANGE people (18 18 BYSCORE REV WITHSCORES
      assertThat(redis.zrevrangebyscoreWithScores("people", from(including(18), excluding(18)))).isEmpty();
      // ZRANGE people (18 (18 BYSCORE REV WITHSCORES
      assertThat(redis.zrevrangebyscoreWithScores("people", from(excluding(18), excluding(18)))).isEmpty();
      // ZRANGE people 17.5 -11 BYSCORE REV WITHSCORES
      assertThat(redis.zrevrangebyscoreWithScores("people", from(including(-11), including(17.5))))
            .containsExactly(
                  just(17, "vittorio"),
                  just(1, "ryan"),
                  just(1, "gustavo"),
                  just(1, "dan"),
                  just(-10, "tristan"),
                  just(-10, "joselie")
            );
      // ZRANGE people 1 -10 BYSCORE REV WITHSCORES
      assertThat(redis.zrevrangebyscoreWithScores("people", from(including(-10), including(1))))
            .containsExactly(
                  just(1, "ryan"),
                  just(1, "gustavo"),
                  just(1, "dan"),
                  just(-10, "tristan"),
                  just(-10, "joselie")
            );
      // ZRANGE people 17 (-10 BYSCORE REV WITHSCORES
      assertThat(redis.zrevrangebyscoreWithScores("people", from(excluding(-10), including(17))))
            .containsExactly(
                  just(17, "vittorio"),
                  just(1, "ryan"),
                  just(1, "gustavo"),
                  just(1, "dan")
            );
      // ZRANGE people (17 (-10 BYSCORE REV WITHSCORES
      assertThat(redis.zrevrangebyscoreWithScores("people", from(excluding(-10), excluding(17))))
            .containsExactly(
                  just(1, "ryan"),
                  just(1, "gustavo"),
                  just(1, "dan")
            );
      // ZRANGE people (17 (-10 BYSCORE REV WITHSCORES LIMIT 1 1
      assertThat(redis.zrevrangebyscoreWithScores("people", from(excluding(-10), excluding(17)), Limit.create(1, 1)))
            .containsExactly(
                  just(1, "gustavo")
            );
      // ZRANGE people (17 (-10 BYSCORE REV WITHSCORES LIMIT 0 0
      assertThat(redis.zrevrangebyscoreWithScores("people", from(excluding(-10), excluding(17)), Limit.create(1, 0)))
            .isEmpty();
      // ZRANGE people (17 (-10 BYSCORE REV WITHSCORES LIMIT 1 -1
      assertThat(redis.zrevrangebyscoreWithScores("people", from(excluding(-10), excluding(17)), Limit.create(1, -1)))
            .containsExactly(
                  just(1, "gustavo"),
                  just(1, "dan")
            );
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zrevrangebyscore("another", Range.<Double>unbounded()));
   }

   public void testZRANGEbyLex() {
      // ZRANGE people - + BYLEX
      assertThat(redis.zrangebylex("not_existing", Range.unbounded())).isEmpty();
      // ZADD people 0 antonio 0 bautista 0 carlos 0 carmela 0 carmelo 0 daniel 0 daniela 0 debora 0 ernesto 0 gonzalo 0 luis
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(0, "antonio"),
            just(0, "bautista"),
            just(0, "carlos"),
            just(0, "carmela"),
            just(0, "carmelo"),
            just(0, "daniel"),
            just(0, "daniela"),
            just(0, "debora"),
            just(0, "ernesto"),
            just(0, "gonzalo"),
            just(0, "luis")
      );
      // ZRANGE people - + BYLEX
      assertThat(redis.zrangebylex("people", Range.unbounded())).
            containsExactly("antonio", "bautista", "carlos", "carmela",
            "carmelo", "daniel", "daniela", "debora", "ernesto", "gonzalo", "luis");
      // ZRANGE people - [debora BYLEX
      assertThat(redis.zrangebylex("people", from(unbounded(), including("debora"))))
            .containsExactly("antonio", "bautista", "carlos", "carmela", "carmelo", "daniel", "daniela", "debora");
      // ZRANGE people - (debora BYLEX
      assertThat(redis.zrangebylex("people", from(unbounded(), excluding("debora"))))
            .containsExactly("antonio", "bautista", "carlos", "carmela", "carmelo", "daniel", "daniela");
      // ZRANGE people [carmelo + BYLEX
      assertThat(redis.zrangebylex("people", from(including("carmelo"), unbounded())))
            .containsExactly("carmelo", "daniel", "daniela", "debora", "ernesto", "gonzalo", "luis");
      // ZRANGE people (carmelo + BYLEX
      assertThat(redis.zrangebylex("people", from(excluding("carmelo"), unbounded())))
            .containsExactly("daniel", "daniela", "debora", "ernesto", "gonzalo", "luis");
      // ZRANGE people (ca (d BYLEX
      assertThat(redis.zrangebylex("people", from(excluding("ca"), excluding("d"))))
            .containsExactly("carlos", "carmela", "carmelo");
      // ZRANGE people (ca (de BYLEX
      assertThat(redis.zrangebylex("people", from(excluding("ca"), excluding("de"))))
            .containsExactly("carlos", "carmela", "carmelo", "daniel", "daniela");
      // ZRANGE people (co (di BYLEX
      assertThat(redis.zrangebylex("people", from(excluding("co"), excluding("di"))))
            .containsExactly("daniel", "daniela", "debora");
      // ZRANGE people - + BYLEX LIMIT 2 4
      assertThat(redis.zrangebylex("people", Range.unbounded(), Limit.create(2,  4)))
            .containsExactly("carlos", "carmela", "carmelo", "daniel");
      // ZRANGE people - + BYLEX LIMIT 0 0
      assertThat(redis.zrangebylex("people", Range.unbounded(), Limit.create(0,  0))).isEmpty();
      // ZRANGE people - + BYLEX LIMIT 5 -1
      assertThat(redis.zrangebylex("people", Range.unbounded(), Limit.create(5,  -1)))
            .containsExactly("daniel", "daniela", "debora", "ernesto", "gonzalo", "luis");
      // ZRANGE people (ca (d BYLEX LIMIT 2 4
      assertThat(redis.zrangebylex("people", from(excluding("ca"), excluding("d")), Limit.create(1,  1)))
            .containsExactly("carmela");

      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zrangebylex("another", Range.unbounded()));
   }

   public void testZREVRANGEbyLex() {
      // ZRANGE people + - BYLEX REV
      assertThat(redis.zrevrangebylex("not_existing", Range.unbounded())).isEmpty();
      // ZADD people 0 antonio 0 bautista 0 carlos 0 carmela 0 carmelo 0 daniel 0 daniela 0 debora 0 ernesto 0 gonzalo 0 luis
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(0, "antonio"),
            just(0, "bautista"),
            just(0, "carlos"),
            just(0, "carmela"),
            just(0, "carmelo"),
            just(0, "daniel"),
            just(0, "daniela"),
            just(0, "debora"),
            just(0, "ernesto"),
            just(0, "gonzalo"),
            just(0, "luis")
      );
      // ZRANGE people + - BYLEX REV
      assertThat(redis.zrevrangebylex("people", Range.unbounded()))
            .containsExactly("luis", "gonzalo", "ernesto", "debora", "daniela", "daniel",
                  "carmelo", "carmela", "carlos", "bautista", "antonio");
      // ZRANGE people [debora - BYLEX REV
      assertThat(redis.zrevrangebylex("people", from(unbounded(), including("debora"))))
            .containsExactly("debora", "daniela", "daniel", "carmelo", "carmela", "carlos", "bautista", "antonio");
      // ZRANGE people (debora - BYLEX REV
      assertThat(redis.zrevrangebylex("people", from(unbounded(), excluding("debora"))))
            .containsExactly("daniela", "daniel", "carmelo", "carmela", "carlos", "bautista", "antonio");
      // ZRANGE people + [debora BYLEX REV
      assertThat(redis.zrevrangebylex("people", from(including("debora"), unbounded())))
            .containsExactly("luis", "gonzalo", "ernesto", "debora");
      // ZRANGE people + (debora BYLEX REV
      assertThat(redis.zrevrangebylex("people", from(excluding("debora"), unbounded())))
            .containsExactly("luis", "gonzalo", "ernesto");
      // ZRANGE people (d (ca BYLEX REV
      assertThat(redis.zrevrangebylex("people", from(excluding("ca"), excluding("d"))))
            .containsExactly("carmelo", "carmela", "carlos");
      // ZRANGE people (de (ca BYLEX REV
      assertThat(redis.zrevrangebylex("people", from(excluding("ca"), excluding("de"))))
            .containsExactly("daniela", "daniel", "carmelo", "carmela", "carlos");
      // ZRANGE people (di (co BYLEX REV
      assertThat(redis.zrevrangebylex("people", from(excluding("co"), excluding("di"))))
            .containsExactly("debora", "daniela", "daniel");
      // ZRANGE people + - BYLEX REV LIMIT 2 4
      assertThat(redis.zrevrangebylex("people", Range.unbounded(), Limit.create(2,  4)))
            .containsExactly("ernesto", "debora", "daniela", "daniel");
      // ZRANGE people + - BYLEX REV LIMIT 0 0
      assertThat(redis.zrevrangebylex("people", Range.unbounded(), Limit.create(0,  0))).isEmpty();
      // ZRANGE people + - BYLEX REV LIMIT 5 -1
      assertThat(redis.zrevrangebylex("people", Range.unbounded(), Limit.create(5,  -1)))
            .containsExactly("daniel", "carmelo", "carmela", "carlos", "bautista", "antonio");
      // ZRANGE people (d (ca BYLEX REV LIMIT 2 1
      assertThat(redis.zrevrangebylex("people", from(excluding("ca"), excluding("d")),
            Limit.create(2,  1)))
            .containsExactly("carlos");

      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zrevrangebylex("another", Range.unbounded()));
   }

   public void testZRANGESTORE() {
      assertThat(redis.zrangestore("npeople", "not_existing", create(0L, 1L))).isEqualTo(0);
      assertThat(redis.exists("npeople")).isEqualTo(0);

      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(0, "antonio"),
            just(0, "bautista"),
            just(0, "carlos"),
            just(0, "carmela"),
            just(0, "carmelo"),
            just(0, "daniel"),
            just(0, "daniela"),
            just(0, "debora"),
            just(0, "ernesto"),
            just(0, "gonzalo"),
            just(0, "luis")
      );
      assertThat(redis.zrange("people", 1, 5)).containsExactly("bautista", "carlos", "carmela", "carmelo", "daniel");
      assertThat(redis.zrangestore("npeople", "people", create(1L, 5L))).isEqualTo(5);
      assertThat(redis.zrange("npeople", 0, -1)).containsExactly("bautista", "carlos", "carmela", "carmelo", "daniel");
      assertThat(redis.zrangestorebylex("npeople", "people", create("deb", "luisa"), Limit.create(1, 2))).isEqualTo(2);
      assertThat(redis.zrange("npeople", 0, -1)).containsExactly("ernesto", "gonzalo");
      assertThat(redis.zrangestorebylex("npeople", "people", create("zi", "zu"), Limit.unlimited())).isEqualTo(0);
      assertThat(redis.exists("npeople")).isEqualTo(0);

      redis.zadd("infinipeople", ZAddArgs.Builder.ch(),
            just(1, "galder"),
            just(2, "dan"),
            just(3, "adrian"),
            just(3.5, "radim"),
            just(4, "tristan"),
            just(4, "vittorio"),
            just(5, "pedro"),
            just(5, "fabio"),
            just(6, "jose"),
            just(6, "ryan"),
            just(6, "anna")
      );
      assertThat(redis.zrangestorebyscore("remaining", "infinipeople", from(including(3.4), including(6.8)), Limit.create(1, -1)))
            .isEqualTo(7);
      assertThat(redis.zrangeWithScores("remaining", 0, -1))
            .containsExactly(
                  just(4, "tristan"),
                  just(4, "vittorio"),
                  just(5, "fabio"),
                  just(5, "pedro"),
                  just(6, "anna"),
                  just(6, "jose"),
                  just(6, "ryan"));
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zrevrangebylex("another", Range.unbounded()));
   }

   public void testZRANK() {
      assertThat(redis.zrank("people", "tristan")).isNull();
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(1, "galder"),
            just(2, "dan"),
            just(3, "adrian"),
            just(3.5, "radim"),
            just(4, "tristan"),
            just(4, "vittorio"),
            just(5, "pedro"),
            just(5, "fabio"),
            just(6, "jose"),
            just(6, "ryan"),
            just(6, "anna"));
      assertThat(redis.zrank("people", "ramona")).isNull();
      assertThat(redis.zrank("people", "tristan")).isEqualTo(4);
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zrank("another","tristan"));
   }

   public void testZREVRANK() {
      assertThat(redis.zrevrank("people", "tristan")).isNull();
      // ZADD people 1 galder 2 dan 3 adrian 3.5 radim 4 tristan 4 vittorio 5 pedro 5 fabio 6 jose 6 ryan 6 anna
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(1, "galder"),
            just(2, "dan"),
            just(3, "adrian"),
            just(3.5, "radim"),
            just(4, "tristan"),
            just(4, "vittorio"),
            just(5, "pedro"),
            just(5, "fabio"),
            just(6, "jose"),
            just(6, "ryan"),
            just(6, "anna"));
      assertThat(redis.zrevrank("people", "ramona")).isNull();
      assertThat(redis.zrevrank("people", "tristan")).isEqualTo(6);
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zrevrank("another","tristan"));
   }

   public void testZMSCORE() {
      // ZMSCORE not_existing not_existing
      List<Double> notExistingSortedSetCallResult = redis.zmscore("not_existing", "no_existing");
      assertThat(notExistingSortedSetCallResult).hasSize(1);
      assertThat(notExistingSortedSetCallResult.get(0)).isNull();
      // ZADD people -10 tristan 1 ryan 17 vittorio 18.9 fabio 18.9 jose 18.9 katia 21.9 marc
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(-10, "tristan"),
            just(1, "ryan"),
            just(17, "vittorio"),
            just(18.9, "fabio"),
            just(18.9, "jose"),
            just(18.9, "katia"),
            just(21.9, "marc"));
      // ZMSCORE people maria juana
      List<Double> notExistingMemberCallResult = redis.zmscore("people", "maria", "juana");
      assertThat(notExistingMemberCallResult).hasSize(2);
      assertThat(notExistingMemberCallResult.get(0)).isNull();
      assertThat(notExistingMemberCallResult.get(1)).isNull();
      // ZMSCORE people jose marc juliette
      assertThat(redis.zmscore("people", "jose", "juliette", "marc")).containsExactly(18.9, null, 21.9);
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zmscore("another", "tristan"));
   }

   public void testZDIFF() {
      assertThat(redis.zdiff("result", "not_existing1", "not_existing2")).isEmpty();
      // ZADD s1 1 a 2 b 3 c
      redis.zadd("s1", ZAddArgs.Builder.ch(),
            just(1, "a"),
            just(2, "b"),
            just(3, "c"));
      assertThat(redis.zdiff("s1")).containsExactly("a", "b", "c");
      assertThat(redis.zdiffWithScores("s1")).containsExactly(
            just(1, "a"),
            just(2, "b"),
            just(3, "c"));
      // ZADD s3 3 a 3 b 7 g
      redis.zadd("s2", ZAddArgs.Builder.ch(),
            just(3, "a"),
            just(3, "b"),
            just(7, "g"));
      assertThat(redis.zdiff("s1", "s2")).containsExactly("c");
      assertThat(redis.zdiffWithScores("s1", "s2")).containsExactly(
            just(3, "c"));
      assertThat(redis.zdiffWithScores("s2", "s1")).containsExactly(
            just(7, "g"));
      // ZADD s3 3 a 3 b 7 g
      redis.zadd("s2", ZAddArgs.Builder.ch(),
            just(3, "a"),
            just(3, "b"),
            just(7, "g"));
      assertThat(redis.zdiffWithScores("not_existing", "s2", "s1")).isEmpty();
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zdiff("another"));
   }

   public void testZDIFFSTORE() {
      assertThat(redis.zdiffstore("result", "not_existing1", "not_existing2")).isZero();
      // ZADD s1 1 a 2 b 3 c
      redis.zadd("s1", ZAddArgs.Builder.ch(),
            just(1, "a"),
            just(2, "b"),
            just(3, "c"));
      // ZADD s2 1 a 2 b 3 c 4 d 5 e 6 f 7 g
      redis.zadd("s2", ZAddArgs.Builder.ch(),
            just(1, "a"),
            just(2, "b"),
            just(7, "g"));
      // ZADD s3 1 a 2 b 7 g 8 h
      redis.zadd("s3", ZAddArgs.Builder.ch(),
            just(1, "a"),
            just(2, "b"),
            just(7, "g"),
            just(8, "h"));

      assertThat(redis.zdiffstore("result", "s1", "s2", "s3")).isEqualTo(1);
      assertThat(redis.zrangeWithScores("result", 0, -1)).containsExactly(just(3, "c"));
      assertThat(redis.zdiffstore("result", "s2", "s3")).isEqualTo(0);
      assertThat(redis.zrangeWithScores("result", 0, -1)).isEmpty();
      assertThat(redis.exists("result")).isEqualTo(0);

      assertWrongType(() -> redis.set("another1", "tristan"), () ->  redis.zdiffstore("another1", "s1"));
      assertWrongType(() -> redis.set("another2", "tristan"), () ->  redis.zdiffstore("people",  "another2"));
   }

   public void testZINCRBY() {
      assertThat(redis.zincrby("people",  30, "tristan")).isEqualTo(30);
      assertThat(redis.zrangeWithScores("people", 0, -1)).containsExactly(just(30, "tristan"));
      assertThat(redis.zincrby("people",  2, "tristan")).isEqualTo(32);
      assertThat(redis.zrangeWithScores("people", 0, -1)).containsExactly(just(32, "tristan"));
      assertThat(redis.zincrby("people",  -4, "tristan")).isEqualTo(28);
      assertThat(redis.zrangeWithScores("people", 0, -1)).containsExactly(just(28, "tristan"));
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zincrby("another",  30, "tristan"));
   }

   public void testZUNION() {
      // ZUNION 1 s1
      assertThat(redis.zunion("s1")).isEmpty();
      // ZADD s1 1 a 2 b 3 c
      assertThat(redis.zadd("s1",
            just(1, "a"),
            just(2, "b"),
            just(3, "c"))).isEqualTo(3);
      // ZUNION 1 s1
      assertThat(redis.zunion("s1")).containsExactly("a", "b", "c");
      // ZUNION 1 s1 WITHSCORES
      assertThat(redis.zunionWithScores("s1")).containsExactly(
            just(1, "a"),
            just(2, "b"),
            just(3, "c"));
      // ZUNION 1 s1 WEIGHTS 2 WITHSCORES
      assertThat(redis.zunionWithScores(weights(2), "s1")).containsExactly(
            just(2, "a"),
            just(4, "b"),
            just(6, "c"));
      // ZADD s2 2 a 3 b 4 c 5 d
      assertThat(redis.zadd("s2",
            just(2, "a"),
            just(3, "b"),
            just(4, "c"),
            just(5, "d"))).isEqualTo(4);
      // ZUNION 2 s1 s2 WITHSCORES
      assertThat(redis.zunionWithScores("s1", "s2")).containsExactly(
            just(3, "a"),
            just(5, "b"),
            just(5, "d"),
            just(7, "c"));
      // ZUNION 2 s1 s2 WITHSCORES
      assertThat(redis.zunionWithScores(sum(),"s1", "s2")).containsExactly(
            just(3, "a"),
            just(5, "b"),
            just(5, "d"),
            just(7, "c"));
      // ZUNION 2 s1 s2 WEIGHTS 3 2 WITHSCORES
      assertThat(redis.zunionWithScores(weights(3, 2), "s1", "s2")).containsExactly(
            just(7, "a"),
            just(10, "d"),
            just(12, "b"),
            just(17, "c"));
      // ZUNION 2 s1 s2 WITHSCORES AGGREGATE MAX
      assertThat(redis.zunionWithScores(max(), "s1", "s2")).containsExactly(
            just(2, "a"),
            just(3, "b"),
            just(4, "c"),
            just(5, "d"));
      // ZUNION 2 s1 s2 WITHSCORES AGGREGATE MIN
      assertThat(redis.zunionWithScores(min(), "s1", "s2")).containsExactly(
            just(1, "a"),
            just(2, "b"),
            just(3, "c"),
            just(5, "d"));
      // ZUNION 2 s1 s2 WEIGHTS 3 2 WITHSCORES AGGREGATE MIN
      assertThat(redis.zunionWithScores(weights(3, 2).min(), "s1", "s2")).containsExactly(
            just(3, "a"),
            just(6, "b"),
            just(8, "c"),
            just(10, "d"));
      // ZUNION 2 s1 s2 WEIGHTS 3 2 WITHSCORES AGGREGATE MAX
      assertThat(redis.zunionWithScores(weights(3, 2).max(), "s1", "s2")).containsExactly(
            just(4, "a"),
            just(6, "b"),
            just(9, "c"),
            just(10, "d"));
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zunion("another", "people"));
   }

   public void testZUNIONSTORE() {
      // ZUNIONSTORE s1 1 s1
      assertThat(redis.zunionstore("s1", "s1")).isZero();
      // EXISTS s1
      assertThat(redis.exists("s1")).isZero();
      // ZADD s1 1 a 2 b 3 c
      assertThat(redis.zadd("s1",
            just(1, "a"),
            just(2, "b"),
            just(3, "c"))).isEqualTo(3);
      assertThat(redis.zunionstore("s1", "s1")).isEqualTo(3);
      // ZRANGE s1 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("s1", 0, -1)).containsExactly(
            just(1, "a"),
            just(2, "b"),
            just(3, "c"));
      // ZUNIONSTORE s2 1 s1
      assertThat(redis.zunionstore("s2", "s1")).isEqualTo(3);
      // ZRANGE s2 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("s2", 0, -1)).containsExactly(
            just(1, "a"),
            just(2, "b"),
            just(3, "c"));
      // ZUNIONSTORE s2 1 s1 WEIGHTS 3
      assertThat(redis.zunionstore("s2", ZStoreArgs.Builder.weights(3), "s1")).isEqualTo(3);
      // ZRANGE s2 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("s2", 0, -1)).containsExactly(
            just(3, "a"),
            just(6, "b"),
            just(9, "c"));
      // ZUNIONSTORE s3 2 s1 s2 AGGREGATE MIN
      assertThat(redis.zunionstore("s3", ZStoreArgs.Builder.min(), "s1", "s2")).isEqualTo(3);
      // ZRANGE s3 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("s3", 0, -1)).containsExactly(
            just(1, "a"),
            just(2, "b"),
            just(3, "c"));
      // ZUNIONSTORE s3 2 s1 s2 AGGREGATE MAX
      assertThat(redis.zunionstore("s3", ZStoreArgs.Builder.max(), "s1", "s2")).isEqualTo(3);
      // ZRANGE s3 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("s3", 0, -1)).containsExactly(
            just(3, "a"),
            just(6, "b"),
            just(9, "c"));
      // ZUNIONSTORE s3 2 s1 s2 AGGREGATE SUM
      assertThat(redis.zunionstore("s3", ZStoreArgs.Builder.sum(), "s1", "s2")).isEqualTo(3);
      // ZRANGE s3 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("s3", 0, -1)).containsExactly(
            just(4, "a"),
            just(8, "b"),
            just(12, "c"));
      // ZADD s3 2 d 7 f
      assertThat(redis.zadd("s3",
            just(2, "d"),
            just(7, "f"))).isEqualTo(2);
      // ZUNIONSTORE s4 3 s1 s2 s3
      assertThat(redis.zunionstore("s4", "s1", "s2", "s3")).isEqualTo(5);
      // ZRANGE s4 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("s4", 0, -1)).containsExactly(
            just(2, "d"),
            just(7, "f"),
            just(8, "a"),
            just(16, "b"),
            just(24, "c")
      );
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zunionstore("another", "people"));
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zunionstore("people", "another"));
   }

   public void testZINTER() {
      // ZINTER 1 s1
      assertThat(redis.zinter("s1")).isEmpty();
      // ZADD s1 1 a 2 b 3 c
      assertThat(redis.zadd("s1",
            just(1, "a"),
            just(2, "b"),
            just(3, "c"))).isEqualTo(3);
      // ZINTER 1 s1
      assertThat(redis.zinter("s1")).containsExactly("a", "b", "c");
      // ZINTER 1 s1 WITHSCORES
      assertThat(redis.zinterWithScores("s1")).containsExactly(
            just(1, "a"),
            just(2, "b"),
            just(3, "c"));
      // ZINTER 2 s1 s1 WITHSCORES
      assertThat(redis.zinterWithScores("s1", "s1")).containsExactly(
            just(2, "a"),
            just(4, "b"),
            just(6, "c"));
      // ZINTER 3 s1 s1 s1 WITHSCORES WEIGHTS 1 2 3
      assertThat(redis.zinterWithScores(ZAggregateArgs.Builder.weights(1, 2, 3), "s1", "s1", "s1")).containsExactly(
            just(6, "a"),
            just(12, "b"),
            just(18, "c"));
      // ZINTER 2 s1 s2 WITHSCORES
      assertThat(redis.zinterWithScores("s1", "s2")).isEmpty();
      // ZADD s2 3 a 8 b 1 d
      assertThat(redis.zadd("s2",
            just(3, "a"),
            just(8, "b"),
            just(1, "d"))).isEqualTo(3);
      // ZINTER 2 s1 s2 WITHSCORES
      assertThat(redis.zinterWithScores( "s1", "s2")).containsExactly(
            just(4, "a"),
            just(10, "b"));
      // ZINTER 2 s1 s2 WITHSCORES AGGREGATE MIN
      assertThat(redis.zinterWithScores( ZAggregateArgs.Builder.min(), "s1", "s2")).containsExactly(
            just(1, "a"),
            just(2, "b"));
      // ZINTER 2 s1 s2 WITHSCORES AGGREGATE MAX
      assertThat(redis.zinterWithScores( ZAggregateArgs.Builder.max(), "s1", "s2")).containsExactly(
            just(3, "a"),
            just(8, "b"));
      // ZINTER 2 s1 s2 WITHSCORES AGGREGATE MAX WEIGHTS 5 1
      assertThat(redis.zinterWithScores(ZAggregateArgs.Builder.weights(5, 1).max(), "s1", "s2")).containsExactly(
            just(5, "a"),
            just(10, "b"));
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zinter("another", "people"));
   }

   public void testZINTERSTORE() {
      // ZINTER 1 s1
      assertThat(redis.zinterstore("s1", "s1")).isZero();
      // EXISTS s1
      assertThat(redis.exists("s1")).isZero();
      // ZADD s1 1 a 2 b 3 c
      assertThat(redis.zadd("s1",
            just(1, "a"),
            just(2, "b"),
            just(3, "c"))).isEqualTo(3);
      // ZINTERSTORE s1 1 s1
      assertThat(redis.zinterstore("s1", "s1")).isEqualTo(3);
      // ZRANGE s1 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("s1", 0, -1)).containsExactly(
            just(1, "a"),
            just(2, "b"),
            just(3, "c"));
      // ZINTERSTORE s2 1 s1
      assertThat(redis.zinterstore("s2", "s1")).isEqualTo(3);
      // ZRANGE s2 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("s2", 0, -1)).containsExactly(
            just(1, "a"),
            just(2, "b"),
            just(3, "c"));
      // ZINTERSTORE s2 1 s1 WEIGHTS 2
      assertThat(redis.zinterstore("s2", ZStoreArgs.Builder.weights(2), "s1")).isEqualTo(3);
      // ZRANGE s2 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("s2", 0, -1)).containsExactly(
            just(2, "a"),
            just(4, "b"),
            just(6, "c"));
      // ZINTERSTORE s2 1 s1 WEIGHTS 2
      assertThat(redis.zinterstore("s2", ZStoreArgs.Builder.weights(2), "s1")).isEqualTo(3);
      // ZINTERSTORE s2 2 s1 s3
      assertThat(redis.zinterstore("s2", "s1", "s3")).isZero();
      // EXISTS s2
      assertThat(redis.exists("s2")).isZero();
      // ZADD s3 3 a 8 b 1 d
      assertThat(redis.zadd("s3",
            just(3, "a"),
            just(8, "b"),
            just(1, "d"))).isEqualTo(3);
      // ZINTERSTORE s2 2 s1 s3
      assertThat(redis.zinterstore("s2", "s1", "s3")).isEqualTo(2);
      // ZRANGE s2 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("s2", 0, -1)).containsExactly(
            just(4, "a"),
            just(10, "b"));
      // ZINTERSTORE s2 2 s1 s3 AGGREGATE MIN
      assertThat(redis.zinterstore("s2", ZStoreArgs.Builder.min(), "s1", "s3")).isEqualTo(2);
      // ZRANGE s2 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("s2", 0, -1)).containsExactly(
            just(1, "a"),
            just(2, "b"));
      // ZINTERSTORE s2 2 s1 s3 AGGREGATE MAX
      assertThat(redis.zinterstore("s2", ZStoreArgs.Builder.max(), "s1", "s3")).isEqualTo(2);
      // ZRANGE s2 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("s2", 0, -1)).containsExactly(
            just(3, "a"),
            just(8, "b"));
      // ZINTERSTORE s2 2 s1 s3 WEIGHTS 5 1 AGGREGATE MAX
      assertThat(redis.zinterstore("s2", ZStoreArgs.Builder.weights(5, 1).max(), "s1", "s3")).isEqualTo(2);
      // ZRANGE s2 0 -1 WITHSCORES
      assertThat(redis.zrangeWithScores("s2", 0, -1)).containsExactly(
            just(5, "a"),
            just(10, "b"));
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zinterstore("another", "people"));
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zinterstore("people", "another"));
   }

   public void testZREM() {
      // ZREM not_existing not_existing
      assertThat(redis.zrem("not_existing", "value")).isZero();
      // ZADD people -10 tristan 1 ryan 17 vittorio 18.9 fabio 18.9 jose 18.9 katia 21.9 marc
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(-10, "tristan"),
            just(1, "ryan"),
            just(17, "vittorio"),
            just(18.9, "fabio"),
            just(18.9, "jose"),
            just(18.9, "katia"),
            just(21.9, "marc"));
      // ZREM people tristan marc fabio pedro
      assertThat(redis.zrem("people", "tristan", "marc", "fabio", "pedro")).isEqualTo(3);
      // ZRANGE people 0 -1
      assertThat(redis.zrange("people", 0, -1)).containsExactly("ryan", "vittorio", "jose", "katia");
      // ZREM people ryan vittorio jose katia
      assertThat(redis.zrem("people", "ryan", "vittorio", "jose", "katia")).isEqualTo(4);
      // ZRANGE people 0 -1
      assertThat(redis.zrange("people", 0, -1)).isEmpty();
      // EXISTS people
      assertThat(redis.exists("people")).isZero();
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zrem("another", "tristan"));
   }

   public void testZREMRANGEBYRANK() {
      // ZREMRANGEBYRANK not_existing 0 -1
      assertThat(redis.zremrangebyrank("not_existing", 0, -1)).isZero();
      // ZADD people -10 tristan 1 ryan 17 vittorio 18.9 fabio 18.9 jose 18.9 katia 21.9 marc
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(-10, "tristan"),
            just(1, "ryan"),
            just(17, "vittorio"),
            just(18.9, "fabio"),
            just(18.9, "jose"),
            just(18.9, "katia"),
            just(21.9, "marc"));
      // ZREMRANGEBYRANK people 0 -1
      assertThat(redis.zremrangebyrank("people", 0, -1)).isEqualTo(7);
      // EXISTS people
      assertThat(redis.exists("people")).isZero();
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(-10, "tristan"),
            just(1, "ryan"),
            just(17, "vittorio"),
            just(18.9, "fabio"),
            just(18.9, "jose"),
            just(18.9, "katia"),
            just(21.9, "marc"));
      // ZREMRANGEBYRANK people 2 6
      assertThat(redis.zremrangebyrank("people", 7, 8)).isZero();
      // ZREMRANGEBYRANK people 2 6
      assertThat(redis.zremrangebyrank("people", 2, 6)).isEqualTo(5);
      // ZREMRANGEBYRANK people -3 -3
      assertThat(redis.zremrangebyrank("people", -3, -3)).isZero();
      // ZRANGE people 0 -1
      assertThat(redis.zrange("people", 0, -1)).containsExactly("tristan", "ryan");
      // ZREMRANGEBYRANK people -3 -2
      assertThat(redis.zremrangebyrank("people", -3, -2)).isEqualTo(1);
      // ZRANGE people 0 -1
      assertThat(redis.zrange("people", 0, -1)).containsExactly("ryan");
      // ZREMRANGEBYRANK people 1 1
      assertThat(redis.zremrangebyrank("people", 1, 1)).isZero();
      // ZREMRANGEBYRANK people -1 -1
      assertThat(redis.zremrangebyrank("people", -1, -1)).isEqualTo(1);
      // EXISTS people
      assertThat(redis.exists("people")).isZero();
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zremrangebyrank("another", 0, -1));
   }

   public void testZREMRANGEBYSCORE() {
      // ZREMRANGEBYSCORE not_existing -inf +inf
      assertThat(redis.zremrangebyscore("not_existing", Range.unbounded())).isZero();
      // ZADD people -10 tristan 1 ryan 17 vittorio 18.9 fabio 18.9 jose 18.9 pedro 18.9 juan 18.9 katia 21.9 marc
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(-10, "tristan"),
            just(1, "ryan"),
            just(17, "vittorio"),
            just(18.9, "fabio"),
            just(18.9, "jose"),
            just(18.9, "pedro"),
            just(18.9, "juan"),
            just(18.9, "katia"),
            just(21.9, "marc"));
      // ZREMRANGEBYSCORE not_existing -inf +inf
      assertThat(redis.zremrangebyscore("people", Range.unbounded())).isEqualTo(9);
      // EXISTS people
      assertThat(redis.exists("people")).isZero();
      // ZADD people -10 tristan 1 ryan 17 vittorio 18.9 fabio 18.9 jose 18.9 pedro 18.9 juan 18.9 katia 21.9 marc
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(-10, "tristan"),
            just(1, "ryan"),
            just(17, "vittorio"),
            just(18.9, "fabio"),
            just(18.9, "jose"),
            just(18.9, "pedro"),
            just(18.9, "juan"),
            just(18.9, "katia"),
            just(21.9, "marc"));
      // ZREMRANGEBYSCORE people -inf 18.9
      assertThat(redis.zremrangebyscore("people", from(unbounded(), including(18.9)))).isEqualTo(8);
      // ZRANGE people 0 -1
      assertThat(redis.zrange("people", 0, -1)).containsExactly("marc");
      // ZREMRANGEBYSCORE people -11 22
      assertThat(redis.zremrangebyscore("people", create(-11, 22))).isEqualTo(1);
      // EXISTS people
      assertThat(redis.exists("people")).isZero();
      // ZADD people -10 tristan 1 ryan 17 vittorio 18.9 fabio 18.9 jose 18.9 pedro 18.9 juan 18.9 katia 21.9 marc
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(-10, "tristan"),
            just(1, "ryan"),
            just(17, "vittorio"),
            just(18.9, "fabio"),
            just(18.9, "jose"),
            just(18.9, "pedro"),
            just(18.9, "juan"),
            just(18.9, "katia"),
            just(21.9, "marc"));
      assertThat(redis.zremrangebyscore("people", Range.unbounded())).isEqualTo(9);
      // EXISTS people
      assertThat(redis.exists("people")).isZero();
      // ZREMRANGEBYSCORE another 0 1
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zremrangebyscore("another", 0, 1));
   }

   public void testZREMRANGEBYLEX() {
      // ZREMRANGEBYLEX not_existing - +
      assertThat(redis.zremrangebylex("not_existing", Range.unbounded())).isZero();
      // ZADD people 0 antonio 0 bautista 0
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(0, "antonio"),
            just(0, "bautista")
      );
      // ZREMRANGEBYLEX not_existing - +
      assertThat(redis.zremrangebylex("people", Range.unbounded())).isEqualTo(2);
      // ZADD people 0 antonio 0 bautista 0 carlos 0 carmela 0 carmelo 0 daniel 0 daniela 0 debora 0 ernesto 0 gonzalo 0 luis
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(0, "antonio"),
            just(0, "bautista"),
            just(0, "carlos"),
            just(0, "carmela"),
            just(0, "carmelo"),
            just(0, "daniel"),
            just(0, "daniela"),
            just(0, "debora"),
            just(0, "ernesto"),
            just(0, "gonzalo"),
            just(0, "luis")
      );
      // ZREMRANGEBYLEX people - (carlos
      assertThat(redis.zremrangebylex("people", from(unbounded(), excluding("carlos")))).isEqualTo(2);
      // ZREMRANGEBYLEX people - [daniel
      assertThat(redis.zremrangebylex("people", from(unbounded(), including("daniel")))).isEqualTo(4);
      // ZREMRANGEBYLEX people (debora +
      assertThat(redis.zremrangebylex("people", from(excluding("debora"), unbounded()))).isEqualTo(3);
      // ZADD people 0 antonio 0 bautista 0 carlos 0 carmela 0 carmelo 0 daniel 0 daniela 0 debora 0 ernesto 0 gonzalo 0 luis
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(0, "antonio"),
            just(0, "bautista"),
            just(0, "carlos"),
            just(0, "carmela"),
            just(0, "carmelo"),
            just(0, "daniel"),
            just(0, "daniela"),
            just(0, "debora"),
            just(0, "ernesto"),
            just(0, "gonzalo"),
            just(0, "luis")
      );
      // ZREMRANGEBYLEX people [debora +
      assertThat(redis.zremrangebylex("people", from(including("debora"), unbounded()))).isEqualTo(4);
      // ZREMRANGEBYLEX people [bau [dan
      assertThat(redis.zremrangebylex("people", from(including("bau"), including("dan")))).isEqualTo(4);
      // ZREMRANGEBYLEX people (bau (dan
      assertThat(redis.zremrangebylex("people", from(excluding("bau"), excluding("dan")))).isZero();
      // ZREMRANGEBYLEX people (antonio (daniela
      assertThat(redis.zremrangebylex("people", from(excluding("antonio"), excluding("daniela")))).isEqualTo(1);
      // ZREMRANGEBYLEX people (antonia (danielo
      assertThat(redis.zremrangebylex("people", from(excluding("antonia"), excluding("danielo")))).isEqualTo(2);
      // EXISTS people
      assertThat(redis.exists("people")).isZero();
      // ZREMRANGEBYLEX another - +
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zremrangebylex("another", Range.unbounded()));
   }

   public void testZINTERCARD() {
      // ZINTERCARD 1 s1
      assertThat(redis.zintercard("s1")).isZero();

      // ZADD s1 1 a 2 b 3 c
      assertThat(redis.zadd("s1",
            just(1, "a"),
            just(2, "b"),
            just(3, "c"))).isEqualTo(3);

      // ZINTERCARD 1 s1
      assertThat(redis.zintercard("s1")).isEqualTo(3);

      // ZINTERCARD 1 s1
      assertThat(redis.zintercard("s1")).isEqualTo(3);

      // ZINTERCARD 2 s1 s2
      assertThat(redis.zintercard("s1", "s2")).isZero();

      // ZADD s2 1 a 2 b 3 c
      assertThat(redis.zadd("s2",
            just(1, "a"),
            just(2, "b"),
            just(3, "c"))).isEqualTo(3);

      // ZINTERCARD 2 s1 s2
      assertThat(redis.zintercard("s1", "s2")).isEqualTo(3);

      // ZINTERCARD 2 s1 s2 LIMIT 0
      assertThat(redis.zintercard(0,"s1", "s2")).isEqualTo(3);

      // ZINTERCARD 2 s1 s2 LIMIT 4
      assertThat(redis.zintercard(4,"s1", "s2")).isEqualTo(3);

      // ZINTERCARD 2 s1 s2 LIMIT 2
      assertThat(redis.zintercard(2,"s1", "s2")).isEqualTo(2);

      // ZINTERCARD 1 another
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zintercard("another"));
   }

   public void testZRANDMEMBER() {
      // ZRANDMEMBER people
      assertThat(redis.zrandmember("people")).isNull();
      // ZRANDMEMBER people 1
      assertThat(redis.zrandmember("people", 1)).isEmpty();
      // ZRANDMEMBER people 1 WITHSCORES
      assertThat(redis.zrandmemberWithScores("people", 1)).isEmpty();
      // ZADD people 1 galder 2 dan 3 adrian 3.5 radim 4 tristan 4 vittorio 5 pedro 5 fabio 6 jose 6 ryan 6 anna
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(1, "galder"),
            just(2, "dan"),
            just(3, "adrian"),
            just(3.5, "radim"),
            just(4, "tristan"),
            just(4, "vittorio"),
            just(5, "pedro"),
            just(5, "fabio"),
            just(6, "jose"),
            just(6, "ryan"),
            just(6, "anna"));
      assertThat(redis.zrandmember("people")).containsAnyOf("galder", "dan", "adrian", "radim", "tristan",
            "vittorio", "pedro", "fabio", "jose", "ryan", "anna");
      assertThat(redis.zrandmember("people", 2))
            .containsAnyOf("galder", "dan", "adrian", "radim", "tristan",
            "vittorio", "pedro", "fabio", "jose", "ryan", "anna");
      assertThat(redis.zrandmember("people", 11))
            .containsExactlyInAnyOrder("galder", "dan", "adrian", "radim", "tristan",
                  "vittorio", "pedro", "fabio", "jose", "ryan", "anna");
      assertThat(redis.zrandmemberWithScores("people", 11))
            .containsExactlyInAnyOrder(just(1, "galder"),
                  just(2, "dan"),
                  just(3, "adrian"),
                  just(3.5, "radim"),
                  just(4, "tristan"),
                  just(4, "vittorio"),
                  just(5, "pedro"),
                  just(5, "fabio"),
                  just(6, "jose"),
                  just(6, "ryan"),
                  just(6, "anna"));
      assertThat(redis.zrandmember("people", 13))
            .containsExactlyInAnyOrder("galder", "dan", "adrian", "radim", "tristan",
                  "vittorio", "pedro", "fabio", "jose", "ryan", "anna");
      assertThat(redis.zrandmember("people", -20)).hasSize(20);
      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zrandmember("another"));
   }

   public void testZLEXCOUNT() {
      // ZLEXCOUNT people - +
      assertThat(redis.zlexcount("people", Range.unbounded())).isZero();
      // ZADD people 0 antonio 0 bautista 0 carlos 0 carmela 0 carmelo 0 daniel 0 daniela 0 debora 0 ernesto 0 gonzalo 0 luis
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(0, "antonio"),
            just(0, "bautista"),
            just(0, "carlos"),
            just(0, "carmela"),
            just(0, "carmelo"),
            just(0, "daniel"),
            just(0, "daniela"),
            just(0, "debora"),
            just(0, "ernesto"),
            just(0, "gonzalo"),
            just(0, "luis")
      );
      // ZLEXCOUNT people - +
      assertThat(redis.zlexcount("people", Range.unbounded())).isEqualTo(11);
      // ZLEXCOUNT people (daniel +
      assertThat(redis.zlexcount("people", from(excluding("daniel"), unbounded()))).isEqualTo(5);
      // ZLEXCOUNT people [daniel +
      assertThat(redis.zlexcount("people", from(including("daniel"), unbounded()))).isEqualTo(6);
      // ZLEXCOUNT people - (carmela
      assertThat(redis.zlexcount("people", from(unbounded(), excluding("carmela")))).isEqualTo(3);
      // ZLEXCOUNT people - [carmela
      assertThat(redis.zlexcount("people", from(unbounded(), including("carmela")))).isEqualTo(4);
      // ZLEXCOUNT people (bautista (carmela
      assertThat(redis.zlexcount("people", from(excluding("bautista"), excluding("carmela")))).isEqualTo(1);
      // ZLEXCOUNT people [bautista [carmela
      assertThat(redis.zlexcount("people", from(including("bautista"), including("carmela")))).isEqualTo(3);
      // ZLEXCOUNT people [bautista (carmela
      assertThat(redis.zlexcount("people", from(including("bautista"), excluding("carmela")))).isEqualTo(2);
      // ZLEXCOUNT people (bautista [carmela
      assertThat(redis.zlexcount("people", from(excluding("bautista"), including("carmela")))).isEqualTo(2);
      // ZLEXCOUNT people (lars (luna
      assertThat(redis.zlexcount("people", from(excluding("lars"), excluding("luna")))).isEqualTo(1);
      // ZLEXCOUNT people [lars [lana
      assertThat(redis.zlexcount("people", from(including("lars"), including("lana")))).isZero();

      assertWrongType(() -> redis.set("another", "tristan"), () ->  redis.zrandmember("another"));
   }

   public void testZSCAN() {
      // ZADD people 1 tristan 2 vittorio 3 pedro 4 fabio 5 anna
      redis.zadd("people", ZAddArgs.Builder.ch(),
            just(1, "tristan"),
            just(2, "vittorio"),
            just(2, "pedro"),
            just(5, "fabio"),
            just(5, "anna"));
      // ZSCAN people 0
      assertThat(redis.zscan("people").getValues())
            .containsExactly(
                  just(1, "tristan"),
                  just(2, "pedro"),
                  just(2, "vittorio"),
                  just(5, "anna"),
                  just(5, "fabio"));

      // ZSCAN people 0 MATCH tris*
      assertThat(redis.zscan("people", ScanArgs.Builder.matches("tris*")).getValues())
            .containsExactly(
                  just(1, "tristan"));

      // ZSCAN people 0 MATCH nonsense
      assertThat(redis.zscan("people", ScanArgs.Builder.matches("nonsense")).getValues())
            .isEmpty();
   }
}
