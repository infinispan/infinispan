package org.infinispan.server.resp;

import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.sync.RedisCommands;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static io.lettuce.core.Range.Boundary.excluding;
import static io.lettuce.core.Range.Boundary.including;
import static io.lettuce.core.Range.Boundary.unbounded;
import static io.lettuce.core.Range.from;
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
      assertThat(redis.zrangestore("npeople", "not_existing", Range.create(0L, 1L))).isEqualTo(0);
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
      assertThat(redis.zrangestore("npeople", "people", Range.create(1L, 5L))).isEqualTo(5);
      assertThat(redis.zrange("npeople", 0, -1)).containsExactly("bautista", "carlos", "carmela", "carmelo", "daniel");
      assertThat(redis.zrangestorebylex("npeople", "people", Range.create("deb", "luisa"), Limit.create(1, 2))).isEqualTo(2);
      assertThat(redis.zrange("npeople", 0, -1)).containsExactly("ernesto", "gonzalo");
      assertThat(redis.zrangestorebylex("npeople", "people", Range.create("zi", "zu"), Limit.unlimited())).isEqualTo(0);
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
      assertThat(redis.zrangestorebyscore("remaining", "infinipeople", Range.from(including(3.4), including(6.8)), Limit.create(1, -1)))
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
}
