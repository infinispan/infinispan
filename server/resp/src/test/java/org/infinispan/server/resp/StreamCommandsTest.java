package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.resp.test.RespTestingUtil.ADMIN;
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.security.Security;
import org.infinispan.server.resp.test.RawRespConnection;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import io.lettuce.core.CopyArgs;
import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.XTrimArgs;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.output.IntegerOutput;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

/**
 * RESP stream commands testing.
 *
 * <p>
 * The tests mirror the Redis test suite file {@code tests/unit/type/stream.tcl} from the Redis unstable branch. Test
 * methods are named after the upstream tests, in the same order. Tests that depend on Redis internals not applicable
 * to this implementation (radix tree nodes, AOF/RDB persistence mechanics, IDMP, consumer groups) are either adapted,
 * with a comment noting the deviation, or omitted.
 * </p>
 *
 * @since 16.4
 */
@Test(groups = "functional", testName = "server.resp.StreamCommandsTest")
public class StreamCommandsTest extends SingleNodeRespBaseTest {

   private RedisCommands<String, String> redis;

   @Override
   public Object[] factory() {
      return new Object[] {
            new StreamCommandsTest(),
            new StreamCommandsTest().withAuthorization(),
      };
   }

   @BeforeMethod
   public void initConnection() {
      redis = redisConnection.sync();
   }

   // Upstream: XADD wrong number of args
   public void testXaddWrongNumberOfArgs() {
      assertThatThrownBy(() -> xaddRaw("mystream"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("wrong number of arguments for 'xadd' command");
      assertThatThrownBy(() -> xaddRaw("mystream", "*"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("wrong number of arguments for 'xadd' command");
      assertThatThrownBy(() -> xaddRaw("mystream", "*", "field"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("wrong number of arguments for 'xadd' command");
   }

   // Upstream: XADD can add entries into a stream that XRANGE can fetch
   public void testXaddCanAddEntriesIntoStreamThatXrangeCanFetch() {
      assertThat(redis.xadd("mystream", "item", "1", "value", "a")).isNotNull();
      assertThat(redis.xadd("mystream", "item", "2", "value", "b")).isNotNull();
      assertThat(redis.xlen("mystream")).isEqualTo(2);

      List<Object> items = xrangeRaw("mystream");
      assertThat(items).hasSize(2);
      assertThat(entryFields(items, 0)).containsExactly("item", "1", "value", "a");
      assertThat(entryFields(items, 1)).containsExactly("item", "2", "value", "b");
   }

   // Upstream: XADD IDs are incremental
   public void testXaddIdsAreIncremental() {
      String id1 = redis.xadd("mystream", "item", "1", "value", "a");
      String id2 = redis.xadd("mystream", "item", "2", "value", "b");
      String id3 = redis.xadd("mystream", "item", "3", "value", "c");
      assertThat(compareIds(id1, id2)).isEqualTo(-1);
      assertThat(compareIds(id2, id3)).isEqualTo(-1);
   }

   // Upstream: XADD IDs are incremental when ms is the same as well
   public void testXaddIdsAreIncrementalWhenMsIsTheSameAsWell() {
      redis.multi();
      redis.xadd("mystream", "item", "1", "value", "a");
      redis.xadd("mystream", "item", "2", "value", "b");
      redis.xadd("mystream", "item", "3", "value", "c");
      io.lettuce.core.TransactionResult results = redis.exec();
      assertThat(results.wasRolledBack()).isFalse();
      assertThat(results.size()).isEqualTo(3);
      String id1 = (String) results.get(0);
      String id2 = (String) results.get(1);
      String id3 = (String) results.get(2);
      assertThat(compareIds(id1, id2)).isEqualTo(-1);
      assertThat(compareIds(id2, id3)).isEqualTo(-1);
   }

   // Upstream: XADD IDs correctly report an error when overflowing
   public void testXaddIdsCorrectlyReportAnErrorWhenOverflowing() {
      redis.del("mystream");
      redis.xadd("mystream", new XAddArgs().id("18446744073709551615-18446744073709551615"), "a", "b");
      assertThatThrownBy(() -> redis.xadd("mystream", "c", "d"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR");
   }

   // Upstream: XADD auto-generated sequence is incremented for last ID
   public void testXaddAutoGeneratedSequenceIsIncrementedForLastId() {
      redis.del("mystream");
      String id1 = redis.xadd("mystream", new XAddArgs().id("123-456"), "item", "1", "value", "a");
      String id2 = redis.xadd("mystream", new XAddArgs().id("123-*"), "item", "2", "value", "b");
      assertThat(seq(id2)).isEqualTo(457);
      assertThat(compareIds(id1, id2)).isEqualTo(-1);
   }

   // Upstream: XADD auto-generated sequence is zero for future timestamp ID
   public void testXaddAutoGeneratedSequenceIsZeroForFutureTimestampId() {
      redis.del("mystream");
      String id1 = redis.xadd("mystream", new XAddArgs().id("123-456"), "item", "1", "value", "a");
      String id2 = redis.xadd("mystream", new XAddArgs().id("789-*"), "item", "2", "value", "b");
      assertThat(seq(id2)).isEqualTo(0);
      assertThat(compareIds(id1, id2)).isEqualTo(-1);
   }

   // Upstream: XADD auto-generated sequence can't be smaller than last ID
   public void testXaddAutoGeneratedSequenceCantBeSmallerThanLastId() {
      redis.del("mystream");
      redis.xadd("mystream", new XAddArgs().id("123-456"), "item", "1", "value", "a");
      assertThatThrownBy(() -> redis.xadd("mystream", new XAddArgs().id("42-*"), "item", "2", "value", "b"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR");
   }

   // Upstream: XADD auto-generated sequence can't overflow
   public void testXaddAutoGeneratedSequenceCantOverflow() {
      redis.del("mystream");
      redis.xadd("mystream", new XAddArgs().id("1-18446744073709551615"), "a", "b");
      assertThatThrownBy(() -> redis.xadd("mystream", new XAddArgs().id("1-*"), "c", "d"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR");
   }

   // Upstream: XADD 0-* should succeed
   public void testXaddZeroStarShouldSucceed() {
      redis.del("mystream");
      String id = redis.xadd("mystream", new XAddArgs().id("0-*"), "a", "b");
      assertThat(seq(id)).isEqualTo(1);
   }

   // Upstream: XADD with MAXLEN option
   public void testXaddWithMaxlenOption() {
      redis.del("mystream");
      for (int j = 0; j < 1000; j++) {
         redis.xadd("mystream", new XAddArgs().maxlen(5), "xitem", String.valueOf(j));
      }
      assertThat(redis.xlen("mystream")).isEqualTo(5);
      List<io.lettuce.core.StreamMessage<String, String>> res = redis.xrange("mystream", Range.unbounded());
      int expected = 995;
      for (io.lettuce.core.StreamMessage<String, String> entry : res) {
         assertThat(entry.getBody().get("xitem")).isEqualTo(String.valueOf(expected));
         expected++;
      }
   }

   // Upstream: XADD with MAXLEN option and the '=' argument
   public void testXaddWithMaxlenOptionAndEqualArgument() {
      redis.del("mystream");
      for (int j = 0; j < 1000; j++) {
         redis.xadd("mystream", new XAddArgs().maxlen(5).exactTrimming(), "xitem", String.valueOf(j));
      }
      assertThat(redis.xlen("mystream")).isEqualTo(5);
   }

   /**
    * Upstream: XADD with MAXLEN option and the '~' argument. Adapted: the approximate trimming is implemented as an
    * exact trimming, so the stream length equals the threshold instead of the upstream node-granular 600.
    */
   public void testXaddWithMaxlenOptionAndApproximateArgument() {
      redis.del("mystream");
      for (int j = 0; j < 1000; j++) {
         redis.xadd("mystream", new XAddArgs().maxlen(555).approximateTrimming(), "xitem", String.valueOf(j));
      }
      assertThat(redis.xlen("mystream")).isEqualTo(555);
   }

   // Upstream: XADD with NOMKSTREAM option
   public void testXaddWithNomkstreamOption() {
      redis.del("mystream");
      assertThat(redis.xadd("mystream", new XAddArgs().nomkstream(), "item", "1", "value", "a")).isNull();
      assertThat(redis.exists("mystream")).isEqualTo(0);
      redis.xadd("mystream", "item", "1", "value", "a");
      redis.xadd("mystream", new XAddArgs().nomkstream(), "item", "2", "value", "b");
      assertThat(redis.xlen("mystream")).isEqualTo(2);
      List<Object> items = xrangeRaw("mystream");
      assertThat(entryFields(items, 0)).containsExactly("item", "1", "value", "a");
      assertThat(entryFields(items, 1)).containsExactly("item", "2", "value", "b");
   }

   // Upstream: XADD with MINID option
   public void testXaddWithMinidOption() {
      redis.del("mystream");
      for (int j = 1; j < 1001; j++) {
         String minId = j >= 5 ? String.valueOf(j - 5) : "1000";
         redis.xadd("mystream", new XAddArgs().minId(minId).id(j + "-0"), "xitem", String.valueOf(j));
      }
      assertThat(redis.xlen("mystream")).isEqualTo(6);
      List<io.lettuce.core.StreamMessage<String, String>> res = redis.xrange("mystream", Range.unbounded());
      int expected = 995;
      for (io.lettuce.core.StreamMessage<String, String> entry : res) {
         assertThat(entry.getBody().get("xitem")).isEqualTo(String.valueOf(expected));
         expected++;
      }
   }

   // Upstream: XADD with ID 0-0
   public void testXaddWithIdZeroZero() {
      redis.del("otherstream");
      assertThatThrownBy(() -> redis.xadd("otherstream", new XAddArgs().id("0-0"), "k", "v"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR");
      assertThat(redis.exists("otherstream")).isEqualTo(0);
   }

   /**
    * Upstream: XADD with LIMIT delete entries no more than limit. Adapted: the LIMIT option caps the number of evicted
    * entries, so at most one entry is evicted, instead of the upstream node-granular zero evictions. Utilizes the raw
    * command as the Lettuce client rejects a zero maxlen.
    */
   public void testXaddWithLimitDeleteEntriesNoMoreThanLimit() {
      redis.del("yourstream");
      for (int j = 0; j < 3; j++) {
         redis.xadd("yourstream", "xitem", "v");
      }
      assertThat(xaddRaw("yourstream", "MAXLEN", "~", "0", "LIMIT", "1", "*", "xitem", "v")).isNotNull();
      assertThat(redis.xlen("yourstream")).isEqualTo(3);
   }

   // Upstream: XADD mass insertion and XLEN
   public void testXaddMassInsertionAndXlen() {
      redis.del("mystream");
      insertIntoStreamKey("mystream", 10000);
      assertThat(redis.xlen("mystream")).isEqualTo(10000);
      List<io.lettuce.core.StreamMessage<String, String>> items = redis.xrange("mystream", Range.unbounded());
      for (int j = 0; j < 10000; j++) {
         assertThat(items.get(j).getBody().get("item")).isEqualTo(String.valueOf(j));
      }
   }

   /**
    * Creates the mass-insertion stream utilized by the XRANGE and XREVRANGE tests. The upstream tests share the
    * stream created by the mass insertion test; each test method here recreates it to stay independent.
    */
   private void massInsertStream() {
      redis.del("mystream");
      insertIntoStreamKey("mystream", 10000);
   }

   // Upstream: XRANGE COUNT works as expected
   public void testXrangeCountWorksAsExpected() {
      massInsertStream();
      assertThat(redis.xrange("mystream", Range.unbounded(), Limit.create(0, 10))).hasSize(10);
   }

   // Upstream: XREVRANGE COUNT works as expected
   public void testXrevrangeCountWorksAsExpected() {
      massInsertStream();
      assertThat(redis.xrevrange("mystream", Range.unbounded(), Limit.create(0, 10))).hasSize(10);
   }

   /**
    * An explicit COUNT 0 replies with nil for an existing stream, and an empty array for a missing key, instead of
    * returning all the matching entries. The Lettuce typed API represents both replies as an empty list, so the raw
    * reply bytes are asserted.
    */
   public void testXrangeCountZero() throws IOException {
      redis.del("mystream");
      insertIntoStreamKey("mystream", 10);

      try (RawRespConnection raw = rawConnection()) {
         raw.send("XRANGE mystream - + COUNT 0");
         raw.assertFrame("_\r\n");
         raw.send("XREVRANGE mystream + - COUNT 0");
         raw.assertFrame("_\r\n");
         raw.send("XRANGE missingstream - + COUNT 0");
         raw.assertFrame("*0\r\n");
         raw.send("XREVRANGE missingstream + - COUNT 0");
         raw.assertFrame("*0\r\n");
      }
   }

   // Upstream: XRANGE can be used to iterate the whole stream
   public void testXrangeCanBeUsedToIterateTheWholeStream() {
      massInsertStream();
      String lastId = "0-0";
      int j = 0;
      while (true) {
         List<io.lettuce.core.StreamMessage<String, String>> elements =
               redis.xrange("mystream", Range.create(lastId, "+"), Limit.create(0, 100));
         if (elements.isEmpty()) break;
         for (io.lettuce.core.StreamMessage<String, String> e : elements) {
            assertThat(e.getBody().get("item")).isEqualTo(String.valueOf(j));
            j++;
         }
         lastId = streamNextId(elements.get(elements.size() - 1).getId());
      }
      assertThat(j).isEqualTo(10000);
   }

   // Upstream: XREVRANGE returns the reverse of XRANGE
   public void testXrevrangeReturnsTheReverseOfXrange() {
      massInsertStream();
      List<io.lettuce.core.StreamMessage<String, String>> forward = redis.xrange("mystream", Range.unbounded());
      List<io.lettuce.core.StreamMessage<String, String>> backward = redis.xrevrange("mystream", Range.unbounded());
      assertThat(backward).hasSize(forward.size());
      for (int i = 0; i < forward.size(); i++) {
         assertThat(backward.get(i).getId()).isEqualTo(forward.get(forward.size() - 1 - i).getId());
      }
   }

   // Upstream: XRANGE exclusive ranges
   public void testXrangeExclusiveRanges() {
      List<String> ids = List.of("0-1", "0-18446744073709551615", "1-0", "42-0", "42-42",
            "18446744073709551615-18446744073709551614", "18446744073709551615-18446744073709551615");
      int total = ids.size();
      redis.del("vipstream");
      for (String id : ids) {
         redis.xadd("vipstream", new XAddArgs().id(id), "foo", "bar");
      }
      assertThat(redis.xrange("vipstream", Range.unbounded())).hasSize(total);
      assertThat(redis.xrange("vipstream", Range.from(Range.Boundary.excluding(ids.get(0)), Range.Boundary.including("+"))))
            .hasSize(total - 1);
      assertThat(redis.xrange("vipstream", Range.from(Range.Boundary.including("-"), Range.Boundary.excluding(ids.get(total - 1)))))
            .hasSize(total - 1);
      assertThat(redis.xrange("vipstream", Range.from(Range.Boundary.excluding("0-1"), Range.Boundary.excluding("1-0"))))
            .hasSize(1);
      assertThat(redis.xrange("vipstream", Range.from(Range.Boundary.excluding("1-0"), Range.Boundary.excluding("42-42"))))
            .hasSize(1);

      assertThatThrownBy(() -> redis.xrange("vipstream", Range.from(Range.Boundary.excluding("-"), Range.Boundary.including("+"))))
            .isInstanceOf(RedisCommandExecutionException.class).hasMessageContaining("ERR");
      assertThatThrownBy(() -> redis.xrange("vipstream", Range.from(Range.Boundary.including("-"), Range.Boundary.excluding("+"))))
            .isInstanceOf(RedisCommandExecutionException.class).hasMessageContaining("ERR");
      assertThatThrownBy(() -> redis.xrange("vipstream",
            Range.from(Range.Boundary.excluding("18446744073709551615-18446744073709551615"), Range.Boundary.including("+"))))
            .isInstanceOf(RedisCommandExecutionException.class).hasMessageContaining("ERR");
      assertThatThrownBy(() -> redis.xrange("vipstream",
            Range.from(Range.Boundary.including("-"), Range.Boundary.excluding("0-0"))))
            .isInstanceOf(RedisCommandExecutionException.class).hasMessageContaining("ERR");
   }

   /**
    * Upstream: XRANGE with incomplete IDs. An incomplete start bound has a zero sequence part, and an incomplete end
    * bound the maximal sequence part, so the interval covers every entry with the same milliseconds part.
    */
   public void testXrangeWithIncompleteIds() {
      redis.del("incompletestream");
      redis.xadd("incompletestream", new XAddArgs().id("42-0"), "f", "a");
      redis.xadd("incompletestream", new XAddArgs().id("42-1"), "f", "b");
      redis.xadd("incompletestream", new XAddArgs().id("43-0"), "f", "c");

      assertThat(redis.xrange("incompletestream", Range.create("42", "42")))
            .extracting(io.lettuce.core.StreamMessage::getId).containsExactly("42-0", "42-1");
      assertThat(redis.xrange("incompletestream", Range.create("42", "43")))
            .extracting(io.lettuce.core.StreamMessage::getId).containsExactly("42-0", "42-1", "43-0");
      // An exclusive incomplete start bound excludes only the zero sequence, and an exclusive incomplete end bound
      // excludes only the maximal sequence.
      assertThat(redis.xrange("incompletestream", Range.from(Range.Boundary.excluding("42"), Range.Boundary.including("43"))))
            .extracting(io.lettuce.core.StreamMessage::getId).containsExactly("42-1", "43-0");
      assertThat(redis.xrange("incompletestream", Range.from(Range.Boundary.including("42"), Range.Boundary.excluding("43"))))
            .extracting(io.lettuce.core.StreamMessage::getId).containsExactly("42-0", "42-1", "43-0");
      assertThat(redis.xrange("incompletestream", Range.from(Range.Boundary.excluding("42"), Range.Boundary.excluding("43"))))
            .extracting(io.lettuce.core.StreamMessage::getId).containsExactly("42-1", "43-0");
   }

   // Upstream: XREVRANGE with incomplete IDs, the reverse of the XRANGE intervals.
   public void testXrevrangeWithIncompleteIds() {
      redis.del("incompletestream");
      redis.xadd("incompletestream", new XAddArgs().id("42-0"), "f", "a");
      redis.xadd("incompletestream", new XAddArgs().id("42-1"), "f", "b");
      redis.xadd("incompletestream", new XAddArgs().id("43-0"), "f", "c");

      assertThat(redis.xrevrange("incompletestream", Range.create("42", "42")))
            .extracting(io.lettuce.core.StreamMessage::getId).containsExactly("42-1", "42-0");
      assertThat(redis.xrevrange("incompletestream", Range.from(Range.Boundary.excluding("42"), Range.Boundary.including("43"))))
            .extracting(io.lettuce.core.StreamMessage::getId).containsExactly("43-0", "42-1");
      assertThat(redis.xrevrange("incompletestream", Range.from(Range.Boundary.including("42"), Range.Boundary.excluding("43"))))
            .extracting(io.lettuce.core.StreamMessage::getId).containsExactly("43-0", "42-1", "42-0");
   }

   // Upstream: XREAD with non empty stream
   public void testXreadWithNonEmptyStream() {
      redis.del("mystream");
      insertIntoStreamKey("mystream", 10);
      List<io.lettuce.core.StreamMessage<String, String>> res =
            redis.xread(new XReadArgs().count(1), XReadArgs.StreamOffset.from("mystream", "0-0"));
      assertThat(res).hasSize(1);
      assertThat(res.get(0).getStream()).isEqualTo("mystream");
      assertThat(res.get(0).getBody().get("item")).isEqualTo("0");
   }

   // Upstream: Non blocking XREAD with empty streams
   public void testNonBlockingXreadWithEmptyStreams() {
      // The Lettuce client represents the nil reply as an empty list.
      assertThat(redis.xread(XReadArgs.StreamOffset.from("s1", "0-0"), XReadArgs.StreamOffset.from("s2", "0-0")))
            .isEmpty();
   }

   // Upstream: XREAD with non empty second stream
   public void testXreadWithNonEmptySecondStream() {
      insertIntoStreamKey("stream2", 10);
      List<io.lettuce.core.StreamMessage<String, String>> res = redis.xread(new XReadArgs().count(1),
            XReadArgs.StreamOffset.from("nostream", "0-0"),
            XReadArgs.StreamOffset.from("stream2", "0-0"));
      assertThat(res).hasSize(1);
      assertThat(res.get(0).getStream()).isEqualTo("stream2");
      assertThat(res.get(0).getBody().get("item")).isEqualTo("0");
   }

   // Upstream: XREAD last element from non-empty stream
   public void testXreadLastElementFromNonEmptyStream() {
      redis.del("lestream");
      redis.xadd("lestream", new XAddArgs().id("1-0"), "k1", "v1");
      redis.xadd("lestream", new XAddArgs().id("2-0"), "k2", "v2");
      redis.xadd("lestream", new XAddArgs().id("3-0"), "k3", "v3");

      List<io.lettuce.core.StreamMessage<String, String>> res =
            redis.xread(XReadArgs.StreamOffset.last("lestream"));
      assertThat(res).hasSize(1);
      assertThat(res.get(0).getId()).isEqualTo("3-0");
      assertThat(res.get(0).getBody().get("k3")).isEqualTo("v3");

      redis.xadd("lestream", new XAddArgs().id("3-18446744073709551614"), "k4", "v4");
      redis.xadd("lestream", new XAddArgs().id("3-18446744073709551615"), "k5", "v5");

      res = redis.xread(XReadArgs.StreamOffset.last("lestream"));
      assertThat(res).hasSize(1);
      assertThat(res.get(0).getId()).isEqualTo("3-18446744073709551615");
      assertThat(res.get(0).getBody().get("k5")).isEqualTo("v5");
   }

   /**
    * Upstream: XREAD last element from empty stream. The final part of the upstream test creates the empty stream via
    * {@code XGROUP CREATE ... MKSTREAM}; adapted to utilize {@code XADD MAXLEN 0}, which also creates an empty stream.
    * The Lettuce client represents the nil reply as an empty list.
    */
   public void testXreadLastElementFromEmptyStream() {
      redis.del("lestream");
      assertThat(redis.xread(XReadArgs.StreamOffset.last("lestream"))).isEmpty();

      redis.xadd("lestream", new XAddArgs().id("1-0"), "k1", "v1");
      redis.xdel("lestream", "1-0");
      assertThat(redis.xread(XReadArgs.StreamOffset.last("lestream"))).isEmpty();

      // Create an empty stream, as XGROUP CREATE with MKSTREAM would. The Lettuce client rejects a zero maxlen, so
      // the raw command is utilized.
      redis.del("lestream");
      assertThat(xaddRaw("lestream", "MAXLEN", "0", "*", "k1", "v1")).isNotNull();
      assertThat(redis.xread(XReadArgs.StreamOffset.last("lestream"))).isEmpty();
   }

   // Upstream: XREAD last element with count > 1
   public void testXreadLastElementWithCountGreaterThanOne() {
      redis.del("lestream");
      redis.xadd("lestream", new XAddArgs().id("1-0"), "k1", "v1");
      redis.xadd("lestream", new XAddArgs().id("2-0"), "k2", "v2");
      redis.xadd("lestream", new XAddArgs().id("3-0"), "k3", "v3");

      List<io.lettuce.core.StreamMessage<String, String>> res =
            redis.xread(new XReadArgs().count(3), XReadArgs.StreamOffset.last("lestream"));
      assertThat(res).hasSize(1);
      assertThat(res.get(0).getId()).isEqualTo("3-0");
      assertThat(res.get(0).getBody().get("k3")).isEqualTo("v3");
   }

   // Upstream: XREAD last element from multiple streams
   public void testXreadLastElementFromMultipleStreams() {
      redis.del("lestream1", "lestream2");
      for (int i = 1; i <= 3; i++) {
         redis.xadd("lestream1", new XAddArgs().id(i + "-0"), "k" + i, "v" + i);
         redis.xadd("lestream2", new XAddArgs().id(i + "-0"), "k" + i, "w" + i);
      }

      List<io.lettuce.core.StreamMessage<String, String>> res = redis.xread(
            XReadArgs.StreamOffset.last("lestream1"),
            XReadArgs.StreamOffset.last("lestream2"),
            XReadArgs.StreamOffset.last("lestream3"));
      assertThat(res).hasSize(2);
      assertThat(res.get(0).getStream()).isEqualTo("lestream1");
      assertThat(res.get(0).getId()).isEqualTo("3-0");
      assertThat(res.get(1).getStream()).isEqualTo("lestream2");
      assertThat(res.get(1).getId()).isEqualTo("3-0");
   }

   // Upstream: XREAD MAXCOUNT/MAXSIZE argument validation
   public void testXreadMaxcountMaxsizeArgumentValidation() {
      assertThatThrownBy(() -> xreadRaw("COUNT", "50", "MAXCOUNT", "10", "STREAMS", "mcstream1", "0"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("MAXCOUNT must be greater than or equal to COUNT");
      assertThatThrownBy(() -> xreadRaw("MAXCOUNT", "0", "STREAMS", "mcstream1", "0"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("MAXCOUNT must be a positive integer");
      assertThatThrownBy(() -> xreadRaw("MAXCOUNT", "-1", "STREAMS", "mcstream1", "0"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("MAXCOUNT must be a positive integer");
      assertThatThrownBy(() -> xreadRaw("MAXSIZE", "0", "STREAMS", "mcstream1", "0"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("MAXSIZE must be a positive integer");
      assertThatThrownBy(() -> xreadRaw("MAXSIZE", "-5", "STREAMS", "mcstream1", "0"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("MAXSIZE must be a positive integer");
      assertThatThrownBy(() -> xreadRaw("MAXCOUNT", "foo", "STREAMS", "mcstream1", "0"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("not an integer");
   }

   // XREAD accepts a non-positive COUNT, normalizing it to an unlimited read.
   public void testXreadNonPositiveCountIsUnlimited() {
      redis.del("mystream");
      insertIntoStreamKey("mystream", 10);
      assertThat(xreadTotalEntries(xreadRaw("COUNT", "0", "STREAMS", "mystream", "0"))).isEqualTo(10);
      assertThat(xreadTotalEntries(xreadRaw("COUNT", "-1", "STREAMS", "mystream", "0"))).isEqualTo(10);
   }

   // Upstream: XREAD MAXCOUNT caps total entries across streams
   public void testXreadMaxcountCapsTotalEntriesAcrossStreams() {
      setupMaxcountStreams();
      List<Object> res = xreadRaw("COUNT", "50", "STREAMS", "mcstream1", "mcstream2", "mcstream3", "0", "0", "0");
      assertThat(xreadTotalEntries(res)).isEqualTo(150);
      res = xreadRaw("COUNT", "50", "MAXCOUNT", "80", "STREAMS", "mcstream1", "mcstream2", "mcstream3", "0", "0", "0");
      assertThat(xreadTotalEntries(res)).isEqualTo(80);
   }

   // Upstream: XREAD MAXCOUNT without COUNT caps total entries
   public void testXreadMaxcountWithoutCountCapsTotalEntries() {
      setupMaxcountStreams();
      List<Object> res = xreadRaw("MAXCOUNT", "7", "STREAMS", "mcstream1", "mcstream2", "mcstream3", "0", "0", "0");
      assertThat(xreadTotalEntries(res)).isEqualTo(7);
      assertThat(streamName(res, 0)).isEqualTo("mcstream1");
      // The flattened reply contains a single stream: one key plus its entry list.
      assertThat(res).hasSize(2);
   }

   // Upstream: XREAD MAXCOUNT equal to COUNT behaves like COUNT on a single stream
   public void testXreadMaxcountEqualToCountBehavesLikeCount() {
      setupMaxcountStreams();
      List<io.lettuce.core.StreamMessage<String, String>> a =
            redis.xread(new XReadArgs().count(10), XReadArgs.StreamOffset.from("mcstream1", "0"));
      List<Object> b = xreadRaw("COUNT", "10", "MAXCOUNT", "10", "STREAMS", "mcstream1", "0");
      assertThat(xreadTotalEntries(b)).isEqualTo(a.size());
   }

   // Upstream: XREAD MAXSIZE limits the reply size across streams
   public void testXreadMaxsizeLimitsReplySizeAcrossStreams() {
      setupMaxcountStreams();
      List<Object> full = xreadRaw("STREAMS", "mcstream1", "mcstream2", "mcstream3", "0", "0", "0");
      List<Object> capped = xreadRaw("MAXSIZE", "200", "STREAMS", "mcstream1", "mcstream2", "mcstream3", "0", "0", "0");
      assertThat(xreadTotalEntries(capped)).isLessThan(xreadTotalEntries(full));
      assertThat(xreadTotalEntries(capped)).isGreaterThanOrEqualTo(1);
   }

   // Upstream: XREAD MAXSIZE still returns a single oversized message
   public void testXreadMaxsizeStillReturnsASingleOversizedMessage() {
      redis.del("bigstream");
      redis.xadd("bigstream", new XAddArgs().id("1-1"), "f", "x".repeat(5000));
      List<Object> res = xreadRaw("MAXSIZE", "50", "STREAMS", "bigstream", "0");
      assertThat(xreadTotalEntries(res)).isEqualTo(1);
      assertThat(entryId(res, 0, 0)).isEqualTo("1-1");
   }

   // Upstream: XREAD MAXSIZE and MAXCOUNT together, whichever triggers first
   public void testXreadMaxsizeAndMaxcountTogether() {
      setupMaxcountStreams();
      List<Object> res = xreadRaw("MAXCOUNT", "5", "MAXSIZE", "100000",
            "STREAMS", "mcstream1", "mcstream2", "mcstream3", "0", "0", "0");
      assertThat(xreadTotalEntries(res)).isEqualTo(5);
   }

   // Upstream: XREAD MAXSIZE budget is per-command inside MULTI/EXEC
   public void testXreadMaxsizeBudgetIsPerCommandInsideMultiExec() {
      redis.del("budgetstream");
      for (int i = 0; i < 50; i++) {
         redis.xadd("budgetstream", "f", "v" + i);
      }
      List<Object> solo = xreadRaw("MAXSIZE", "1000", "STREAMS", "budgetstream", "0");
      assertThat(xreadTotalEntries(solo)).isGreaterThan(1);

      redis.multi();
      redis.xrange("budgetstream", Range.unbounded());
      xreadRaw("MAXSIZE", "1000", "STREAMS", "budgetstream", "0");
      io.lettuce.core.TransactionResult results = redis.exec();
      List<Object> inExec = (List<Object>) results.get(1);
      assertThat(xreadTotalEntries(inExec)).isEqualTo(xreadTotalEntries(solo));
   }

   // Upstream: XDEL basic test
   public void testXdelBasicTest() {
      redis.del("somestream");
      redis.xadd("somestream", "foo", "value0");
      String id = redis.xadd("somestream", "foo", "value1");
      redis.xadd("somestream", "foo", "value2");
      assertThat(redis.xdel("somestream", id)).isEqualTo(1);
      assertThat(redis.xlen("somestream")).isEqualTo(2);
      List<io.lettuce.core.StreamMessage<String, String>> result = redis.xrange("somestream", Range.unbounded());
      assertThat(result).hasSize(2);
      assertThat(result.get(0).getBody().get("foo")).isEqualTo("value0");
      assertThat(result.get(1).getBody().get("foo")).isEqualTo("value2");
   }

   // Upstream: XDEL multiply id test
   public void testXdelMultiplyIdTest() {
      redis.del("somestream");
      redis.xadd("somestream", new XAddArgs().id("1-1"), "a", "1");
      redis.xadd("somestream", new XAddArgs().id("1-2"), "b", "2");
      redis.xadd("somestream", new XAddArgs().id("1-3"), "c", "3");
      redis.xadd("somestream", new XAddArgs().id("1-4"), "d", "4");
      redis.xadd("somestream", new XAddArgs().id("1-5"), "e", "5");
      assertThat(redis.xlen("somestream")).isEqualTo(5);
      assertThat(redis.xdel("somestream", "1-1", "1-4", "1-5", "2-1")).isEqualTo(3);
      assertThat(redis.xlen("somestream")).isEqualTo(2);
      List<io.lettuce.core.StreamMessage<String, String>> result = redis.xrange("somestream", Range.unbounded());
      assertThat(result.get(0).getBody().get("b")).isEqualTo("2");
      assertThat(result.get(1).getBody().get("c")).isEqualTo("3");
   }

   /**
    * Upstream: XDEL fuzz test. Adapted: the upstream test populates the stream until the radix tree has more than
    * twenty nodes; this implementation has a flat structure, so a fixed number of entries is utilized. The length
    * assertion waits for the value, as the local read in a replicated cache may briefly lag the write.
    */
   public void testXdelFuzzTest() {
      redis.del("somestream");
      List<String> ids = new ArrayList<>();
      for (int i = 0; i < 1000; i++) {
         ids.add(redis.xadd("somestream", "item", String.valueOf(i)));
      }
      assertThat(redis.xlen("somestream")).isEqualTo(1000);

      // Remove the elements in random order, down to an empty stream, verifying the sanity after every batch.
      java.util.Collections.shuffle(ids);
      int x = ids.size();
      int checked = 0;
      for (String id : ids) {
         assertThat(redis.xdel("somestream", id)).isEqualTo(1);
         x--;
         int expected = x;
         eventually(() -> redis.xlen("somestream") == expected);
         if (x % 100 == 0 || x == 0) {
            assertThat(redis.xrange("somestream", Range.unbounded())).hasSize(x);
            checked++;
         }
      }
      assertThat(checked).isGreaterThan(5);
   }

   // Upstream: XRANGE fuzzing
   public void testXrangeFuzzing() {
      redis.del("fuzzstream");
      Map<String, List<String>> items = new LinkedHashMap<>();
      for (int i = 0; i < 1000; i++) {
         String id = redis.xadd("fuzzstream", "item", String.valueOf(i));
         items.put(id, List.of("item", String.valueOf(i)));
      }

      List<String> ids = new ArrayList<>(items.keySet());
      String lowId = ids.get(0);
      String highId = ids.get(ids.size() - 1);
      for (int j = 0; j < 100; j++) {
         String start = streamRandomId(lowId, highId);
         String end = streamRandomId(lowId, highId);
         List<io.lettuce.core.StreamMessage<String, String>> range =
               redis.xrange("fuzzstream", Range.create(start, end));
         List<Map.Entry<String, List<String>>> expected = streamSimulateXrange(items, start, end);
         assertThat(range).hasSize(expected.size());
         for (int i = 0; i < expected.size(); i++) {
            assertThat(range.get(i).getId()).isEqualTo(expected.get(i).getKey());
         }
      }
   }

   // Upstream: XREVRANGE regression test for issue #5006
   public void testXrevrangeRegressionTestForIssue5006() {
      redis.del("teststream", "teststream2");
      redis.xadd("teststream", new XAddArgs().id("1234567891230-0"), "key1", "value1");
      redis.xadd("teststream", new XAddArgs().id("1234567891240-0"), "key2", "value2");
      redis.xadd("teststream", new XAddArgs().id("1234567891250-0"), "key3", "value3");

      redis.xadd("teststream2", new XAddArgs().id("1234567891230-0"), "key1", "value1");
      redis.xadd("teststream2", new XAddArgs().id("1234567891240-0"), "key1", "value2");
      redis.xadd("teststream2", new XAddArgs().id("1234567891250-0"), "key1", "value3");

      List<io.lettuce.core.StreamMessage<String, String>> res =
            redis.xrevrange("teststream", Range.create("-", "1234567891245"));
      assertThat(res).extracting(io.lettuce.core.StreamMessage::getId)
            .containsExactly("1234567891240-0", "1234567891230-0");

      res = redis.xrevrange("teststream2", Range.create("-", "1234567891245"));
      assertThat(res).extracting(io.lettuce.core.StreamMessage::getId)
            .containsExactly("1234567891240-0", "1234567891230-0");
   }

   // Upstream: XREAD streamID edge (no-blocking)
   public void testXreadStreamIdEdgeNoBlocking() {
      redis.del("x");
      redis.xadd("x", new XAddArgs().id("1-1"), "f", "v");
      redis.xadd("x", new XAddArgs().id("1-18446744073709551615"), "f", "v");
      redis.xadd("x", new XAddArgs().id("2-1"), "f", "v");
      List<io.lettuce.core.StreamMessage<String, String>> res =
            redis.xread(new XReadArgs().block(0), XReadArgs.StreamOffset.from("x", "1-18446744073709551615"));
      assertThat(res).hasSize(1);
      assertThat(res.get(0).getId()).isEqualTo("2-1");
   }

   // Upstream: XADD streamID edge
   public void testXaddStreamIdEdge() {
      redis.del("x");
      // The timestamp must be in the future, so the auto-generated ID continues from it.
      redis.xadd("x", new XAddArgs().id("2577343934890-18446744073709551615"), "f", "v");
      String id = redis.xadd("x", "f2", "v2");
      assertThat(id).isEqualTo("2577343934891-0");
      List<io.lettuce.core.StreamMessage<String, String>> res = redis.xrange("x", Range.unbounded());
      assertThat(res).extracting(io.lettuce.core.StreamMessage::getId)
            .containsExactly("2577343934890-18446744073709551615", "2577343934891-0");
   }

   // Upstream: XADD with partial ID with maximal seq
   public void testXaddWithPartialIdWithMaximalSeq() {
      redis.del("x");
      redis.xadd("x", new XAddArgs().id("1-18446744073709551615"), "f1", "v1");
      assertThatThrownBy(() -> redis.xadd("x", new XAddArgs().id("1-*"), "f2", "v2"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR");
   }

   // Upstream: XTRIM with MAXLEN option basic test
   public void testXtrimWithMaxlenOptionBasicTest() {
      redis.del("mystream");
      for (int j = 0; j < 1000; j++) {
         redis.xadd("mystream", "xitem", String.valueOf(j));
      }
      assertThat(redis.xtrim("mystream", new XTrimArgs().maxlen(666))).isEqualTo(334);
      assertThat(redis.xlen("mystream")).isEqualTo(666);
      assertThat(redis.xtrim("mystream", new XTrimArgs().maxlen(555).exactTrimming())).isEqualTo(111);
      assertThat(redis.xlen("mystream")).isEqualTo(555);
      // Adapted: the approximate trimming is exact, the stream length equals the threshold.
      assertThat(redis.xtrim("mystream", new XTrimArgs().maxlen(444).approximateTrimming())).isEqualTo(111);
      assertThat(redis.xlen("mystream")).isEqualTo(444);
      assertThat(redis.xtrim("mystream", new XTrimArgs().maxlen(400).approximateTrimming())).isEqualTo(44);
      assertThat(redis.xlen("mystream")).isEqualTo(400);
   }

   /**
    * Upstream: XADD with LIMIT consecutive calls. The exact trimming with the LIMIT cap matches the upstream result
    * for the first call, as the LIMIT bounds the number of evicted entries. The second call is adapted: with the
    * exact trimming all the excess entries are evicted, instead of the upstream node-granular evictions.
    */
   public void testXaddWithLimitConsecutiveCalls() {
      redis.del("mystream");
      for (int j = 0; j < 100; j++) {
         redis.xadd("mystream", "xitem", "v");
      }
      redis.xadd("mystream", new XAddArgs().maxlen(55).approximateTrimming().limit(30), "xitem", "v");
      assertThat(redis.xlen("mystream")).isEqualTo(71);
      redis.xadd("mystream", new XAddArgs().maxlen(55).approximateTrimming().limit(30), "xitem", "v");
      assertThat(redis.xlen("mystream")).isEqualTo(55);
   }

   /**
    * Upstream: XTRIM with ~ is limited. Adapted: with the exact trimming, the stream length is the threshold instead
    * of the upstream node-granular two entries.
    */
   public void testXtrimWithTildeIsLimited() {
      redis.del("mystream");
      for (int j = 0; j < 102; j++) {
         redis.xadd("mystream", "xitem", "v");
      }
      assertThat(redis.xtrim("mystream", new XTrimArgs().maxlen(1).approximateTrimming())).isEqualTo(101);
      assertThat(redis.xlen("mystream")).isEqualTo(1);
   }

   // Upstream: XTRIM without ~ is not limited
   public void testXtrimWithoutTildeIsNotLimited() {
      redis.del("mystream");
      for (int j = 0; j < 102; j++) {
         redis.xadd("mystream", "xitem", "v");
      }
      assertThat(redis.xtrim("mystream", new XTrimArgs().maxlen(1).exactTrimming())).isEqualTo(101);
      assertThat(redis.xlen("mystream")).isEqualTo(1);
   }

   // Upstream: XTRIM without ~ and with LIMIT
   public void testXtrimWithoutTildeAndWithLimit() {
      redis.del("mystream");
      for (int j = 0; j < 102; j++) {
         redis.xadd("mystream", "xitem", "v");
      }
      // The Lettuce client only sends LIMIT with the approximate trimming, so the raw command is utilized.
      assertThatThrownBy(() -> xtrimRaw("mystream", "MAXLEN", "1", "LIMIT", "30"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR");
   }

   /**
    * Upstream: XTRIM with LIMIT delete entries no more than limit. Adapted: the LIMIT caps the number of evicted
    * entries, so at most the limit is evicted, instead of the upstream node-granular evictions.
    */
   public void testXtrimWithLimitDeleteEntriesNoMoreThanLimit() {
      redis.del("mystream");
      for (int j = 0; j < 3; j++) {
         redis.xadd("mystream", "xitem", "v");
      }
      assertThat(redis.xtrim("mystream", new XTrimArgs().maxlen(0).approximateTrimming().limit(1))).isEqualTo(1);
      assertThat(redis.xtrim("mystream", new XTrimArgs().maxlen(0).approximateTrimming().limit(2))).isEqualTo(2);
   }

   // Upstream: XTRIM with LIMIT 0 does not limit the trimming, MAXLEN strategy.
   public void testXtrimMaxlenWithLimitZeroIsUnlimited() {
      redis.del("mystream");
      for (int j = 0; j < 5; j++) {
         redis.xadd("mystream", "xitem", "v");
      }
      // The Lettuce client does not send a zero limit, so the raw command is utilized.
      assertThat(xtrimRaw("mystream", "MAXLEN", "~", "0", "LIMIT", "0")).isEqualTo(5);
      assertThat(redis.xlen("mystream")).isEqualTo(0);
   }

   // Upstream: XTRIM with LIMIT 0 does not limit the trimming, MINID strategy.
   public void testXtrimMinidWithLimitZeroIsUnlimited() {
      redis.del("mystream");
      for (int j = 1; j <= 5; j++) {
         redis.xadd("mystream", new XAddArgs().id(j + "-0"), "xitem", "v");
      }
      assertThat(xtrimRaw("mystream", "MINID", "~", "4", "LIMIT", "0")).isEqualTo(3);
      assertThat(redis.xlen("mystream")).isEqualTo(2);
   }

   // Upstream: XADD trimming with LIMIT 0 does not limit the trimming.
   public void testXaddMaxlenWithLimitZeroIsUnlimited() {
      redis.del("mystream");
      for (int j = 0; j < 5; j++) {
         redis.xadd("mystream", "xitem", "v");
      }
      assertThat(xaddRaw("mystream", "MAXLEN", "~", "2", "LIMIT", "0", "*", "xitem", "v")).isNotNull();
      assertThat(redis.xlen("mystream")).isEqualTo(2);
   }

   // Upstream: XTRIM with MINID option
   public void testXtrimWithMinidOption() {
      redis.del("mystream");
      for (int j = 1; j < 1001; j++) {
         redis.xadd("mystream", new XAddArgs().id(j + "-0"), "xitem", "v");
      }
      assertThat(redis.xtrim("mystream", new XTrimArgs().minId("995"))).isEqualTo(994);
      assertThat(redis.xlen("mystream")).isEqualTo(6);
      List<io.lettuce.core.StreamMessage<String, String>> res = redis.xrange("mystream", Range.unbounded());
      assertThat(res).extracting(io.lettuce.core.StreamMessage::getId)
            .containsExactly("995-0", "996-0", "997-0", "998-0", "999-0", "1000-0");
   }

   // Upstream: XTRIM with MINID option, big delta from master record
   public void testXtrimWithMinidOptionBigDeltaFromMasterRecord() {
      redis.del("mystream");
      for (int j = 1; j < 1001; j++) {
         redis.xadd("mystream", new XAddArgs().id(j + "-0"), "xitem", "v");
      }
      assertThat(redis.xtrim("mystream", new XTrimArgs().minId("1"))).isEqualTo(0);
      assertThat(redis.xlen("mystream")).isEqualTo(1000);
   }

   /**
    * Upstream: XTRIM MINID frees a node left with no live entries. Adapted: without internal nodes, the equivalent
    * behavior is the removal of the entries with an ID lower than the threshold.
    */
   public void testXtrimMinidFreesNodeLeftWithNoLiveEntries() {
      redis.del("mystream");
      for (int j = 1; j < 1001; j++) {
         if (j % 3 == 0) {
            continue;
         }
         redis.xadd("mystream", new XAddArgs().id(j + "-0"), "xitem", "v");
      }
      // 667 entries were added, with six of them having an ID lower than 10-0.
      assertThat(redis.xtrim("mystream", new XTrimArgs().minId("10"))).isEqualTo(6);
      assertThat(redis.xlen("mystream")).isEqualTo(661);
      assertThat(redis.xrange("mystream", Range.create("-", "9-0"))).isEmpty();
   }

   // Upstream: XADD can CREATE an empty stream
   public void testXaddCanCreateAnEmptyStream() {
      redis.del("mystream");
      // The Lettuce client rejects a zero maxlen, so the raw command is utilized.
      assertThat(xaddRaw("mystream", "MAXLEN", "0", "*", "a", "b")).isNotNull();
      assertThat(xinfo("mystream")).containsEntry("length", 0L);
   }

   // Upstream: XSETID can set a specific ID
   public void testXsetidCanSetASpecificId() {
      redis.del("mystream");
      // Create an empty stream with one entry added, as the upstream test does. The Lettuce client rejects a zero
      // maxlen, so the raw command is utilized.
      assertThat(xaddRaw("mystream", "MAXLEN", "0", "*", "a", "b")).isNotNull();
      xsetid("mystream", "200-0");
      Map<String, Object> reply = xinfo("mystream");
      assertThat(reply.get("last-generated-id")).isEqualTo("200-0");
      assertThat(reply.get("entries-added")).isEqualTo(1L);
   }

   // Upstream: XSETID cannot SETID with smaller ID
   public void testXsetidCannotSetidWithSmallerId() {
      redis.xadd("mystream", "a", "b");
      assertThatThrownBy(() -> xsetid("mystream", "1-1"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("smaller");
   }

   // Upstream: XSETID cannot SETID on non-existent key
   public void testXsetidCannotSetidOnNonExistentKey() {
      assertThatThrownBy(() -> xsetid("nosuchstream", "1-1"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("no such key");
   }

   // Upstream: XSETID cannot run with an offset but without a maximal tombstone
   public void testXsetidCannotRunWithAnOffsetButWithoutMaximalTombstone() {
      assertThatThrownBy(() -> xsetidRaw("stream", "1-1", "0"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("syntax error");
   }

   // Upstream: XSETID cannot run with a maximal tombstone but without an offset
   public void testXsetidCannotRunWithMaximalTombstoneButWithoutOffset() {
      assertThatThrownBy(() -> xsetidRaw("stream", "1-1", "0-0"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("syntax error");
   }

   // Upstream: XSETID errors on negative offset
   public void testXsetidErrorsOnNegativeOffset() {
      assertThatThrownBy(() -> xsetidRaw("stream", "1-1", "ENTRIESADDED", "-1", "MAXDELETEDID", "0-0"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("must be positive");
   }

   // Upstream: XSETID cannot set the maximal tombstone with larger ID
   public void testXsetidCannotSetTheMaximalTombstoneWithLargerId() {
      redis.del("x");
      redis.xadd("x", new XAddArgs().id("1-0"), "a", "b");
      assertThatThrownBy(() -> xsetidRaw("x", "1-0", "ENTRIESADDED", "1", "MAXDELETEDID", "2-0"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("smaller");
   }

   // Upstream: XSETID cannot set the offset to less than the length
   public void testXsetidCannotSetTheOffsetToLessThanTheLength() {
      redis.del("x");
      redis.xadd("x", new XAddArgs().id("1-0"), "a", "b");
      assertThatThrownBy(() -> xsetidRaw("x", "1-0", "ENTRIESADDED", "0", "MAXDELETEDID", "0-0"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("smaller");
   }

   // Upstream: XSETID cannot set smaller ID than current MAXDELETEDID
   public void testXsetidCannotSetSmallerIdThanCurrentMaxDeletedId() {
      redis.del("x");
      redis.xadd("x", new XAddArgs().id("1-1"), "a", "1");
      redis.xadd("x", new XAddArgs().id("1-2"), "b", "2");
      redis.xadd("x", new XAddArgs().id("1-3"), "c", "3");
      redis.xdel("x", "1-2");
      redis.xdel("x", "1-3");
      assertThat(xinfo("x").get("max-deleted-entry-id")).isEqualTo("1-3");
      assertThatThrownBy(() -> xsetid("x", "1-2"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("smaller");
   }

   // Upstream: XADD advances the entries-added counter and sets the recorded-first-entry-id
   public void testXaddAdvancesTheEntriesAddedCounter() {
      redis.del("x");
      redis.xadd("x", new XAddArgs().id("1-0"), "data", "a");
      Map<String, Object> reply = xinfo("x", "FULL");
      assertThat(reply.get("entries-added")).isEqualTo(1L);
      assertThat(reply.get("recorded-first-entry-id")).isEqualTo("1-0");

      redis.xadd("x", new XAddArgs().id("2-0"), "data", "a");
      reply = xinfo("x", "FULL");
      assertThat(reply.get("entries-added")).isEqualTo(2L);
      assertThat(reply.get("recorded-first-entry-id")).isEqualTo("1-0");
   }

   // Upstream: XDEL/TRIM are reflected by recorded first entry
   public void testXdelTrimAreReflectedByRecordedFirstEntry() {
      redis.del("x");
      for (int i = 1; i <= 5; i++) {
         redis.xadd("x", new XAddArgs().id(i + "-0"), "data", "a");
      }
      Map<String, Object> reply = xinfo("x", "FULL");
      assertThat(reply.get("entries-added")).isEqualTo(5L);
      assertThat(reply.get("recorded-first-entry-id")).isEqualTo("1-0");

      redis.xdel("x", "2-0");
      assertThat(xinfo("x", "FULL").get("recorded-first-entry-id")).isEqualTo("1-0");

      redis.xdel("x", "1-0");
      assertThat(xinfo("x", "FULL").get("recorded-first-entry-id")).isEqualTo("3-0");

      redis.xtrim("x", new XTrimArgs().maxlen(2).exactTrimming());
      assertThat(xinfo("x", "FULL").get("recorded-first-entry-id")).isEqualTo("4-0");
   }

   // Upstream: Maximum XDEL ID behaves correctly
   public void testMaximumXdelIdBehavesCorrectly() {
      redis.del("x");
      redis.xadd("x", new XAddArgs().id("1-0"), "data", "a");
      redis.xadd("x", new XAddArgs().id("2-0"), "data", "b");
      redis.xadd("x", new XAddArgs().id("3-0"), "data", "c");

      assertThat(xinfo("x", "FULL").get("max-deleted-entry-id")).isEqualTo("0-0");

      redis.xdel("x", "2-0");
      assertThat(xinfo("x", "FULL").get("max-deleted-entry-id")).isEqualTo("2-0");

      redis.xdel("x", "1-0");
      assertThat(xinfo("x", "FULL").get("max-deleted-entry-id")).isEqualTo("2-0");
   }

   // TYPE reports the stream type.
   public void testTypeReportsStream() {
      redis.del("typestream");
      redis.xadd("typestream", "f", "v");
      assertThat(redis.type("typestream")).isEqualTo("stream");
   }

   // WRONGTYPE errors for all the stream commands against a non-stream key.
   public void testWrongTypeErrors() {
      assertWrongType(() -> redis.set("stringkey", "value"), () -> redis.xadd("stringkey", "f", "v"));
      assertWrongType(() -> redis.set("stringkey", "value"), () -> redis.xlen("stringkey"));
      assertWrongType(() -> redis.set("stringkey", "value"), () -> redis.xrange("stringkey", Range.unbounded()));
      assertWrongType(() -> redis.set("stringkey", "value"), () -> redis.xrevrange("stringkey", Range.unbounded()));
      assertWrongType(() -> redis.set("stringkey", "value"), () -> redis.xdel("stringkey", "1-1"));
      assertWrongType(() -> redis.set("stringkey", "value"), () -> redis.xtrim("stringkey", new XTrimArgs().maxlen(1)));
      assertWrongType(() -> redis.set("stringkey", "value"),
            () -> redis.xread(XReadArgs.StreamOffset.from("stringkey", "0-0")));
      assertWrongType(() -> redis.set("stringkey", "value"), () -> xsetid("stringkey", "1-1"));
      assertWrongType(() -> redis.set("stringkey", "value"), () -> xinfo("stringkey"));
   }

   // --------------------------------------------------------
   // COPY, following the patterns of the other data types
   // --------------------------------------------------------

   public void testCopyStream() {
      redis.del("copy-stream-src");
      redis.xadd("copy-stream-src", new XAddArgs().id("1-1"), "f1", "v1");
      redis.xadd("copy-stream-src", new XAddArgs().id("2-2"), "f2", "v2");

      assertThat(redis.copy("copy-stream-src", "copy-stream-dst")).isTrue();
      List<io.lettuce.core.StreamMessage<String, String>> entries = redis.xrange("copy-stream-dst", Range.unbounded());
      assertThat(entries).extracting(io.lettuce.core.StreamMessage::getId).containsExactly("1-1", "2-2");
      assertThat(entries.get(0).getBody()).containsEntry("f1", "v1");
      assertThat(entries.get(1).getBody()).containsEntry("f2", "v2");
      // The bookkeeping is copied as well.
      assertThat(xinfo("copy-stream-dst").get("last-generated-id")).isEqualTo("2-2");
   }

   public void testCopyStreamNotPresent() {
      assertThat(redis.copy("copy-stream-missing", "copy-stream-dst")).isFalse();
   }

   public void testCopyStreamEmptyStream() {
      redis.del("copy-stream-empty-src");
      redis.xadd("copy-stream-empty-src", new XAddArgs().id("1-1"), "f", "v");
      redis.xdel("copy-stream-empty-src", "1-1");

      // The key exists as an empty stream after deleting its only entry.
      assertThat(redis.exists("copy-stream-empty-src")).isEqualTo(1);
      assertThat(redis.copy("copy-stream-empty-src", "copy-stream-empty-dst")).isTrue();
      assertThat(redis.exists("copy-stream-empty-dst")).isEqualTo(1);
      assertThat(redis.type("copy-stream-empty-dst")).isEqualTo("stream");
      assertThat(redis.xlen("copy-stream-empty-dst")).isEqualTo(0);
   }

   public void testCopyStreamToExistingKeyWithoutReplace() {
      redis.del("copy-stream-src-nr", "copy-stream-dst-nr");
      redis.xadd("copy-stream-src-nr", new XAddArgs().id("1-1"), "f", "v");
      redis.xadd("copy-stream-dst-nr", new XAddArgs().id("2-2"), "f", "v");

      assertThat(redis.copy("copy-stream-src-nr", "copy-stream-dst-nr")).isFalse();
      assertThat(redis.xrange("copy-stream-dst-nr", Range.unbounded()))
            .extracting(io.lettuce.core.StreamMessage::getId).containsExactly("2-2");
   }

   public void testCopyStreamToExistingKeyWithReplace() {
      redis.del("copy-stream-src-r", "copy-stream-dst-r");
      redis.xadd("copy-stream-src-r", new XAddArgs().id("1-1"), "f", "a");
      redis.xadd("copy-stream-dst-r", new XAddArgs().id("2-2"), "f", "b");

      var copyArgs = new CopyArgs().replace(true);
      assertThat(redis.copy("copy-stream-src-r", "copy-stream-dst-r", copyArgs)).isTrue();
      assertThat(redis.xrange("copy-stream-dst-r", Range.unbounded()))
            .extracting(io.lettuce.core.StreamMessage::getId).containsExactly("1-1");
   }

   public void testCopyStreamWithReplaceToNonExistingKey() {
      redis.del("copy-stream-src-rne");
      redis.xadd("copy-stream-src-rne", new XAddArgs().id("1-1"), "f", "v");

      var copyArgs = new CopyArgs().replace(true);
      assertThat(redis.copy("copy-stream-src-rne", "copy-stream-dst-rne", copyArgs)).isTrue();
      assertThat(redis.xrange("copy-stream-dst-rne", Range.unbounded()))
            .extracting(io.lettuce.core.StreamMessage::getId).containsExactly("1-1");
   }

   public void testCopyStreamToNewDB() {
      createCacheDb1();

      redis.del("copy-stream-db-src");
      redis.xadd("copy-stream-db-src", new XAddArgs().id("1-1"), "f", "v");

      var copyArgs = new CopyArgs().destinationDb(1);
      assertThat(redis.copy("copy-stream-db-src", "copy-stream-db-dst", copyArgs)).isTrue();
      assertThat(redis.xlen("copy-stream-db-dst")).isEqualTo(0);

      redis.select(1);
      assertThat(redis.xrange("copy-stream-db-dst", Range.unbounded()))
            .extracting(io.lettuce.core.StreamMessage::getId).containsExactly("1-1");
      redis.select(0);
   }

   public void testCopyStreamToNewDBWithReplace() {
      createCacheDb1();

      redis.del("copy-stream-dbr-src");
      redis.xadd("copy-stream-dbr-src", new XAddArgs().id("1-1"), "f", "a");

      redis.select(1);
      redis.xadd("copy-stream-dbr-dst", new XAddArgs().id("2-2"), "f", "b");
      redis.select(0);

      var copyArgs = new CopyArgs().destinationDb(1).replace(true);
      assertThat(redis.copy("copy-stream-dbr-src", "copy-stream-dbr-dst", copyArgs)).isTrue();

      redis.select(1);
      assertThat(redis.xrange("copy-stream-dbr-dst", Range.unbounded()))
            .extracting(io.lettuce.core.StreamMessage::getId).containsExactly("1-1");
      redis.select(0);
   }

   /**
    * Creates the cache utilized as the destination database by the {@code COPY} tests with the {@code DB} option. The
    * other data types create the database in a single test and rely on the method ordering; creating it only when
    * missing allows each test to run in isolation.
    */
   private void createCacheDb1() {
      ConfigurationBuilder builder = defaultRespConfiguration();
      amendConfiguration(builder);
      Runnable create = () -> {
         if (!manager(0).cacheExists("1")) {
            manager(0).createCache("1", builder.build());
         }
      };
      if (isAuthorizationEnabled()) {
         Security.doAs(ADMIN, create);
      } else {
         create.run();
      }
   }

   public void testCopyStreamDataIndependence() {
      redis.del("copy-stream-ind-src");
      redis.xadd("copy-stream-ind-src", new XAddArgs().id("1-1"), "f", "a");

      assertThat(redis.copy("copy-stream-ind-src", "copy-stream-ind-dst")).isTrue();

      // Modifying the original does not affect the copy.
      redis.xadd("copy-stream-ind-src", new XAddArgs().id("2-2"), "f", "b");
      assertThat(redis.xrange("copy-stream-ind-dst", Range.unbounded()))
            .extracting(io.lettuce.core.StreamMessage::getId).containsExactly("1-1");

      // Modifying the copy does not affect the original.
      redis.xadd("copy-stream-ind-dst", new XAddArgs().id("3-3"), "f", "c");
      redis.xtrim("copy-stream-ind-dst", new XTrimArgs().maxlen(0).exactTrimming());
      assertThat(redis.xlen("copy-stream-ind-dst")).isEqualTo(0);
      assertThat(redis.xlen("copy-stream-ind-src")).isEqualTo(2);

      // Deleting the original does not affect the copy.
      redis.del("copy-stream-ind-src");
      assertThat(redis.type("copy-stream-ind-dst")).isEqualTo("stream");
   }

   public void testCopyStreamSameKey() {
      redis.del("copy-stream-same");
      redis.xadd("copy-stream-same", new XAddArgs().id("1-1"), "f", "v");

      assertThat(redis.copy("copy-stream-same", "copy-stream-same")).isFalse();
      assertThat(redis.xlen("copy-stream-same")).isEqualTo(1);
   }

   public void testCopyStreamToExistingStringWithoutReplace() {
      redis.del("copy-cross-stream-src");
      redis.xadd("copy-cross-stream-src", new XAddArgs().id("1-1"), "f", "v");
      redis.set("copy-cross-stream-dst", "existing");

      assertThat(redis.copy("copy-cross-stream-src", "copy-cross-stream-dst")).isFalse();
      assertThat(redis.get("copy-cross-stream-dst")).isEqualTo("existing");
   }

   public void testCopyStreamToExistingStringWithReplace() {
      redis.del("copy-cross-stream-r-src");
      redis.xadd("copy-cross-stream-r-src", new XAddArgs().id("1-1"), "f", "v");
      redis.set("copy-cross-stream-r-dst", "existing");

      var copyArgs = new CopyArgs().replace(true);
      assertThat(redis.copy("copy-cross-stream-r-src", "copy-cross-stream-r-dst", copyArgs)).isTrue();
      assertThat(redis.xrange("copy-cross-stream-r-dst", Range.unbounded()))
            .extracting(io.lettuce.core.StreamMessage::getId).containsExactly("1-1");
   }

   // --------------------------------------------------------
   // Helpers, ports of the Tcl helpers in stream.tcl
   // --------------------------------------------------------

   private String xaddRaw(String... args) {
      return redis.dispatch(XADD_KEYWORD, new ValueOutput<>(StringCodec.UTF8), argsOf(args));
   }

   private List<Object> xreadRaw(String... args) {
      return redis.dispatch(XREAD_KEYWORD, new ArrayOutput<>(StringCodec.UTF8), argsOf(args));
   }

   private void xsetid(String key, String id) {
      xsetidRaw(key, id);
   }

   private String xsetidRaw(String... args) {
      return redis.dispatch(XSETID_KEYWORD, new ValueOutput<>(StringCodec.UTF8), argsOf(args));
   }

   private Map<String, Object> xinfo(String key) {
      return xinfo(key, null);
   }

   private Map<String, Object> xinfo(String key, String option) {
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8).add("STREAM").addKey(key);
      if (option != null) {
         args.add(option);
      }
      List<Object> raw = redis.dispatch(XINFO_KEYWORD, new ArrayOutput<>(StringCodec.UTF8), args);
      Map<String, Object> result = new HashMap<>();
      for (int i = 0; i < raw.size(); i += 2) {
         result.put(String.valueOf(raw.get(i)), raw.get(i + 1));
      }
      return result;
   }

   private static CommandArgs<String, String> argsOf(String... args) {
      CommandArgs<String, String> result = new CommandArgs<>(StringCodec.UTF8);
      for (String arg : args) {
         result.add(arg);
      }
      return result;
   }

   private static final ProtocolKeyword XADD_KEYWORD = keyword("XADD");
   private static final ProtocolKeyword XREAD_KEYWORD = keyword("XREAD");
   private static final ProtocolKeyword XSETID_KEYWORD = keyword("XSETID");
   private static final ProtocolKeyword XINFO_KEYWORD = keyword("XINFO");
   private static final ProtocolKeyword XRANGE_KEYWORD = keyword("XRANGE");
   private static final ProtocolKeyword XTRIM_KEYWORD = keyword("XTRIM");

   private static ProtocolKeyword keyword(String name) {
      return new ProtocolKeyword() {
         @Override
         public byte[] getBytes() {
            return name.getBytes(java.nio.charset.StandardCharsets.US_ASCII);
         }

         @Override
         public String toString() {
            return name;
         }
      };
   }

   private void insertIntoStreamKey(String key, int count) {
      for (int j = 0; j < count; j++) {
         redis.xadd(key, "item", String.valueOf(j));
      }
   }

   private void setupMaxcountStreams() {
      redis.del("mcstream1", "mcstream2", "mcstream3");
      for (int i = 0; i < 100; i++) {
         redis.xadd("mcstream1", "f", "v" + i);
         redis.xadd("mcstream2", "f", "v" + i);
         redis.xadd("mcstream3", "f", "v" + i);
      }
   }

   /**
    * Counts the total number of entries across all the streams in an XREAD reply. The Lettuce {@link ArrayOutput}
    * flattens the RESP3 map reply into alternating keys and entry lists.
    */
   private static int xreadTotalEntries(List<Object> reply) {
      int total = 0;
      for (int i = 0; i < reply.size(); i += 2) {
         total += ((List<?>) reply.get(i + 1)).size();
      }
      return total;
   }

   private static String streamName(List<Object> reply, int index) {
      return String.valueOf(reply.get(index * 2));
   }

   private static String entryId(List<Object> reply, int streamIndex, int entryIndex) {
      List<?> entries = (List<?>) reply.get(streamIndex * 2 + 1);
      return String.valueOf(((List<?>) entries.get(entryIndex)).get(0));
   }

   /**
    * Retrieves the flat field-value list of an entry from a raw XRANGE reply, preserving the order. The Lettuce typed
    * API returns a map, which does not guarantee the order.
    */
   private static List<Object> entryFields(List<Object> xrangeReply, int entryIndex) {
      List<?> entry = (List<?>) xrangeReply.get(entryIndex);
      return (List<Object>) entry.get(1);
   }

   private List<Object> xrangeRaw(String key) {
      return redis.dispatch(XRANGE_KEYWORD, new ArrayOutput<>(StringCodec.UTF8),
            new CommandArgs<>(StringCodec.UTF8).addKey(key).add("-").add("+"));
   }

   private Long xtrimRaw(String... args) {
      return redis.dispatch(XTRIM_KEYWORD, new IntegerOutput<>(StringCodec.UTF8), argsOf(args));
   }

   /** Returns the sequence part of the ID. */
   private static long seq(String id) {
      return Long.parseUnsignedLong(id.substring(id.indexOf('-') + 1));
   }

   /** Returns the milliseconds part of the ID. */
   private static long ms(String id) {
      return Long.parseUnsignedLong(id.substring(0, id.indexOf('-')));
   }

   /** The port of the upstream {@code streamCompareID} proc, with unsigned comparison. */
   private static int compareIds(String a, String b) {
      if (a.equals(b)) return 0;
      int msCompare = Long.compareUnsigned(ms(a), ms(b));
      if (msCompare != 0) return msCompare;
      return Long.compareUnsigned(seq(a), seq(b));
   }

   /** The port of the upstream {@code streamNextID} proc. */
   private static String streamNextId(String id) {
      return ms(id) + "-" + (seq(id) + 1);
   }

   /** The port of the upstream {@code streamRandomID} proc. */
   private static String streamRandomId(String minId, String maxId) {
      long randomMs;
      if (ms(maxId) == ms(minId)) {
         randomMs = ms(minId);
      } else {
         randomMs = ms(minId) + ThreadLocalRandom.current().nextLong(ms(maxId) - ms(minId) + 1);
      }
      long randomSeq = ThreadLocalRandom.current().nextLong(1000);
      return randomMs + "-" + randomSeq;
   }

   /** The port of the upstream {@code streamSimulateXRANGE} proc. */
   private static List<Map.Entry<String, List<String>>> streamSimulateXrange(Map<String, List<String>> items,
         String start, String end) {
      List<Map.Entry<String, List<String>>> result = new ArrayList<>();
      for (Map.Entry<String, List<String>> item : items.entrySet()) {
         if (compareIds(item.getKey(), start) >= 0 && compareIds(item.getKey(), end) <= 0) {
            result.add(item);
         }
      }
      return result;
   }
}
