package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.withPrecision;
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.infinispan.commons.time.ControlledTimeService;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.lettuce.core.GetExArgs;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.SetArgs;
import io.lettuce.core.StrAlgoArgs;
import io.lettuce.core.StringMatchResult;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.ValueOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.ProtocolKeyword;

@Test(groups = "functional", testName = "server.resp.StringCommandsTest")
public class StringCommandsTest extends SingleNodeRespBaseTest {

   @Override
   public Object[] factory() {
      return new Object[] {
         new StringCommandsTest(),
         new StringCommandsTest().withAuthorization()
      };
   }

   @Test
   public void testIncrNotPresent() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String nonPresentKey = "incr-notpresent";
      Long newValue = redis.incr(nonPresentKey);
      assertThat(newValue.longValue()).isEqualTo(1L);

      Long nextValue = redis.incr(nonPresentKey);
      assertThat(nextValue.longValue()).isEqualTo(2L);
   }

   @Test
   public void testIncrPresent() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "incr";
      redis.set(key, "12");

      Long newValue = redis.incr(key);
      assertThat(newValue.longValue()).isEqualTo(13L);

      Long nextValue = redis.incr(key);
      assertThat(nextValue.longValue()).isEqualTo(14L);
   }

   @Test
   public void testIncrPresentNotInteger() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "incr-string";
      redis.set(key, "foo");

      assertThatThrownBy(() -> redis.incr(key))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("value is not an integer or out of range");
   }

   @Test
   public void testDecrNotPresent() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String nonPresentKey = "decr-notpresent";
      Long newValue = redis.decr(nonPresentKey);
      assertThat(newValue.longValue()).isEqualTo(-1L);

      Long nextValue = redis.decr(nonPresentKey);
      assertThat(nextValue.longValue()).isEqualTo(-2L);
   }

   @Test
   public void testDecrPresent() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "decr";
      redis.set(key, "12");

      Long newValue = redis.decr(key);
      assertThat(newValue.longValue()).isEqualTo(11L);

      Long nextValue = redis.decr(key);
      assertThat(nextValue.longValue()).isEqualTo(10L);
   }

   public void testIncrResultsNan() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "incr-to-nan";

      redis.set(key, "inf");
      assertThatThrownBy(() -> redis.incrbyfloat(key, -100))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR increment would produce NaN or Infinity");

      redis.set(key, "1");
      assertThatThrownBy(() -> redis.incrbyfloat(key, Double.POSITIVE_INFINITY))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR increment would produce NaN or Infinity");
   }

   @Test
   public void testIncrbyNotPresent() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String nonPresentKey = "incrby-notpresent";
      Long newValue = redis.incrby(nonPresentKey, 42);
      assertThat(newValue.longValue()).isEqualTo(42L);

      Long nextValue = redis.incrby(nonPresentKey, 2);
      assertThat(nextValue.longValue()).isEqualTo(44L);
   }

   @Test
   public void testIncrbyPresent() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "incrby";
      redis.set(key, "12");

      Long newValue = redis.incrby(key, 23);
      assertThat(newValue.longValue()).isEqualTo(35L);

      Long nextValue = redis.incrby(key, 23);
      assertThat(nextValue.longValue()).isEqualTo(58L);
   }

   @Test
   public void testIncrbyPresentNotInteger() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "incrby-string";
      redis.set(key, "foo");
      assertThatThrownBy(() -> redis.incrby(key, 1), "")
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("value is not an integer or out of range");
   }

   @Test
   public void testDecrbyNotPresent() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String nonPresentKey = "decrby-notpresent";
      Long newValue = redis.decrby(nonPresentKey, 42);
      assertThat(newValue.longValue()).isEqualTo(-42L);

      Long nextValue = redis.decrby(nonPresentKey, 2);
      assertThat(nextValue.longValue()).isEqualTo(-44L);
   }

   @Test
   public void testDecrbyPresent() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "decrby";
      redis.set(key, "12");

      Long newValue = redis.decrby(key, 10);
      assertThat(newValue.longValue()).isEqualTo(2L);

      Long nextValue = redis.decrby(key, 10);
      assertThat(nextValue.longValue()).isEqualTo(-8L);
   }

   @Test
   public void testIncrbyFloatNotPresent() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String nonPresentKey = "incrbyfloat-notpresent";
      Double newValue = redis.incrbyfloat(nonPresentKey, 0.42);
      assertThat(newValue.doubleValue()).isEqualTo(0.42, withPrecision(2d));

      Double nextValue = redis.incrbyfloat(nonPresentKey, 0.2);
      assertThat(nextValue.doubleValue()).isEqualTo(0.62, withPrecision(2d));
   }

   @Test
   public void testIncrbyFloatPresent() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "incrbyfloat";
      redis.set(key, "0.12");

      Double newValue = redis.incrbyfloat(key, 0.23);
      assertThat(newValue.doubleValue()).isEqualTo(0.35, withPrecision(2d));

      Double nextValue = redis.incrbyfloat(key, -0.23);
      assertThat(nextValue.doubleValue()).isEqualTo(0.12, withPrecision(2d));
   }

   @Test
   public void testIncrbyFloatPresentNotFloat() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "incrbyfloat-string";
      redis.set(key, "foo");
      assertThatThrownBy(() -> redis.incrbyfloat(key, 0.1), "")
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("value is not a valid float");
   }

   @Test
   public void testAppend() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "append";
      String val = "Hello";
      String app = " World";
      String expect = val + app;
      redis.set(key, val);
      long retVal = redis.append(key, app);
      assertThat(retVal).isEqualTo(expect.length());
      assertThat(redis.get(key)).isEqualTo(expect);
   }

   @Test
   public void testAppendNotPresent() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "append";
      String app = " World";
      String expect = app;
      long retVal = redis.append(key, app);
      assertThat(retVal).isEqualTo(expect.length());
      String val = redis.get(key);
      assertThat(val).isEqualTo(expect);
   }

   public void testGetdel() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "getdel";
      redis.set(key, "value");
      String retval = redis.getdel(key);
      assertThat(retval).isEqualTo("value");
      assertThat(redis.get(key)).isNull();
   }

   @Test
   public void testGetdelNotPresent() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "getdel-notpresent";
      assertThat(redis.getdel(key)).isNull();
   }

   @Test
   public void testStrlen() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "strlen";
      String val = "Hello";
      String app = " World";
      redis.set(key, val);
      assertThat(redis.strlen(key)).isEqualTo(5);
      long retVal = redis.append(key, app);
      assertThat(redis.strlen(key)).isEqualTo(retVal);
   }

   @Test
   public void testStrlenUTF8() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "strlen-nonascii";
      String val = "Euro is €";
      String app = " yen is ¥";
      redis.set(key, val);
      assertThat(redis.strlen(key)).isEqualTo(11);
      // See if append and strlen are consistent
      long retVal = redis.append(key, app);
      assertThat(redis.strlen(key)).isEqualTo(retVal);
   }

   @Test
   public void testStrlenNotPresent() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "strlen-notpresent";
      assertThat(redis.strlen(key)).isEqualTo(0);
   }

   @Test
   public void testLcsCommand() {
      String key1 = "lcs-test-k1";
      String key2 = "lcs-test-k2";

      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(key1, "ohmytext");
      redis.set(key2, "mynewtext");

      CustomStringCommands commands = CustomStringCommands.instance(redisConnection);

      byte[] k1 = key1.getBytes(StandardCharsets.US_ASCII);
      byte[] k2 = key2.getBytes(StandardCharsets.US_ASCII);

      byte[] match = commands.lcs(k1, k2);
      assertThat(new String(match, StandardCharsets.US_ASCII)).isEqualTo("mytext");
      assertThat(commands.lcsLen(k1, k2)).isEqualTo(6);
   }

   @Test(dataProvider = "lcsCases")
   public void testLcs(String v1, String v2, String resp, int[][] idx) {
      String key1 = "lcs-base-1";
      String key2 = "lcs-base-2";
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(key1, v1);
      redis.set(key2, v2);
      StrAlgoArgs args = StrAlgoArgs.Builder.keys(key1, key2);
      StringMatchResult res = redis.stralgoLcs(args);
      assertThat(res.getMatchString()).isEqualTo(resp);
      assertThat(res.getLen()).isZero();
   }

   @Test(dataProvider = "lcsCases")
   public void testLcsLen(String v1, String v2, String resp, int[][] idx) {
      String key1 = "lcs-base-1";
      String key2 = "lcs-base-2";
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(key1, v1);
      redis.set(key2, v2);
      StrAlgoArgs args = StrAlgoArgs.Builder.keys(key1, key2).justLen();
      StringMatchResult res = redis.stralgoLcs(args);
      assertThat(res.getLen()).isEqualTo(resp.length());
      assertThat(res.getMatchString()).isNull();
   }

   @Test(dataProvider = "lcsCases")
   public void testLcsIdx(String v1, String v2, String resp, int[][] idx) {
      String key1 = "lcs-base-1";
      String key2 = "lcs-base-2";
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(key1, v1);
      redis.set(key2, v2);
      StrAlgoArgs args = StrAlgoArgs.Builder.keys(key1, key2).withIdx();
      StringMatchResult res = redis.stralgoLcs(args);
      checkIdx(resp, idx, res, false);
   }

   @Test(dataProvider = "lcsCases")
   public void testLcsIdxWithLen(String v1, String v2, String resp, int[][] idx) {
      String key1 = "lcs-base-1";
      String key2 = "lcs-base-2";
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(key1, v1);
      redis.set(key2, v2);
      StrAlgoArgs args = StrAlgoArgs.Builder.keys(key1, key2).withIdx().withMatchLen();
      StringMatchResult res = redis.stralgoLcs(args);
      checkIdx(resp, idx, res, true);
   }

   @Test(dataProvider = "lcsCasesWithMinLen")
   public void testLcsIdxWithMinLen(String v1, String v2, String resp, int[][] idxs, int minLen) {
      String key1 = "lcs-base-1";
      String key2 = "lcs-base-2";
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(key1, v1);
      redis.set(key2, v2);
      StrAlgoArgs args = StrAlgoArgs.Builder.keys(key1, key2).withIdx().minMatchLen(minLen);
      int[][] idx = Arrays.stream(idxs).filter(pos -> pos.length == 1 || pos[1] - pos[0] >= minLen)
            .toArray(int[][]::new);
      StringMatchResult res = redis.stralgoLcs(args);
      checkIdx(resp, idx, res, false);
   }

   @DataProvider
   public Object[][] lcsCases() {
      return new Object[][] {
            { "GAC", "AGCAT", "AC", new int[][] { { 2, 2, 2, 2 }, { 1, 1, 0, 0 }, { 2 } } },
            { "XMJYAUZ", "MZJAWXU", "MJAU",
                  new int[][] { { 5, 5, 6, 6 }, { 4, 4, 3, 3 }, { 2, 2, 2, 2 }, { 1, 1, 0, 0 }, { 4 } } },
            { "ohmytext", "mynewtext", "mytext", new int[][] { { 4, 7, 5, 8 }, { 2, 3, 0, 1 }, { 6 } } },
            { "ABCBDAB", "BDCABA", "BDAB", new int[][] { { 5, 6, 3, 4 }, { 3, 4, 0, 1 }, { 4 } } },
            { "ABCEZ12 21AAZ", "12ABZ 21AZAZ", "ABZ 21AAZ",
                  new int[][] { { 11, 12, 10, 11 }, { 7, 10, 5, 8 }, { 4, 4, 4, 4 }, { 0, 1, 2, 3 }, { 9 } } }
      };
   }

   @DataProvider
   public Object[][] lcsCasesWithMinLen() {
      var minLengths = new Object[][] { { 1 }, { 2 }, { 4 }, { 10 } };
      var lcsCases = this.lcsCases();
      var result = new Object[lcsCases.length][];
      for (Object[] len : minLengths) {
         for (int i = 0; i < lcsCases.length; i++) {
            result[i] = Stream.concat(Arrays.stream(lcsCases[i]), Arrays.stream(len)).toArray();
         }
      }
      return result;
   }

   private void checkIdx(String resp, int[][] idx, StringMatchResult res, boolean withLen) {
      var matches = res.getMatches();
      assertThat(matches.size()).isEqualTo(idx.length - 1);
      for (int i = 0; i < matches.size(); i++) {
         assertThat(matches.get(i).getA().getStart()).isEqualTo(idx[i][0]);
         assertThat(matches.get(i).getA().getEnd()).isEqualTo(idx[i][1]);
         assertThat(matches.get(i).getB().getStart()).isEqualTo(idx[i][2]);
         assertThat(matches.get(i).getB().getEnd()).isEqualTo(idx[i][3]);
         if (withLen) {
            assertThat(matches.get(i).getMatchLen()).isEqualTo(idx[i][1] - idx[i][0] + 1);
         }
      }
      assertThat(res.getLen()).isEqualTo(resp.length());
      assertThat(res.getMatchString()).isNull();
   }

   @Test
   void testGetRange() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "getrange";
      // SET mykey "A long string for testing"
      redis.set(key, "A long string for testing");
      assertThat(redis.getrange(key, 1, 7)).isEqualTo(" long s");
      // Check negative range
      assertThat(redis.getrange(key, 10, -2)).isEqualTo("ing for testin");
      // Test large range
      assertThat(redis.getrange(key, 0, Long.MAX_VALUE)).isEqualTo("A long string for testing");
      assertThat(redis.getrange(key, Long.MIN_VALUE, Long.MAX_VALUE)).isEqualTo("A long string for testing");
      assertThat(redis.getrange(key, 0, -Long.MAX_VALUE)).isEqualTo("");
      assertThat(redis.getrange(key, Long.MAX_VALUE, -Long.MAX_VALUE)).isEqualTo("");
      // Test single character
      // GETRANGE mykey 0 0
      assertThat(redis.getrange(key, 0, 0)).isEqualTo("A");
      // GETRANGE mykey -1 -1
      assertThat(redis.getrange(key, -1, -1)).isEqualTo("g");
      // GETRANGE mykey 4 4
      assertThat(redis.getrange(key, 4, 4)).isEqualTo("n");
      // GETRANGE mykey -5 -5
      assertThat(redis.getrange(key, -5, -5)).isEqualTo("s");
      // End before beginning
      assertThat(redis.getrange(key, 3, 2)).isEqualTo("");
      // Non-existent entry
      assertThat(redis.getrange("something", 0, 10)).isEqualTo("");
   }

   @Test
   void testGetRangeMultibyte() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "getrange";
      redis.set(key, "Testing with € char");
      var g = redis.getrange(key, 13, 20);
      assertThat(g).isEqualTo("€ char");
      // Check negative range
      assertThat(redis.getrange(key, 10, -2)).isEqualTo("th € cha");
   }

   @Test
   public void testSetrange() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "setrange";
      redis.set(key, "A long string for testing");
      assertThat(redis.setrange(key, 2, "tiny")).isEqualTo(25);
      assertThat(redis.get(key)).isEqualTo("A tiny string for testing");
      // SETRANGE setrange 0 ""
      assertThat(redis.setrange("setrange", 0, "")).isEqualTo(25);
      // EXISTS setrange
      assertThat(redis.exists("setrange")).isEqualTo(1);
      // SETRANGE unkexisting 0 ""
      assertThat(redis.setrange("unkexisting", 0, "")).isEqualTo(0);
      // EXISTS unkexisting
      assertThat(redis.exists("unkexisting")).isEqualTo(0);
   }

   @Test
   public void testSetrangePatchOverflowsLength() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "setrange";
      redis.set(key, "A long string for testing");
      assertThat(redis.setrange(key, 18, "setrange testing")).isEqualTo(34);
      assertThat(redis.get(key)).isEqualTo("A long string for setrange testing");
   }

   @Test
   public void testSetrangeOffsetGreaterThanLength() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "setrange";
      redis.set(key, "A long string for testing");
      assertThat(redis.setrange(key, 30, "my setrange")).isEqualTo(41);
      assertThat(redis.get(key)).isEqualTo("A long string for testing\u0000\u0000\u0000\u0000\u0000my setrange");
   }

   @Test
   public void testSetrangeNotPresent() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "setrange-notpresent";
      assertThat(redis.setrange(key, 5, "my setrange")).isEqualTo(16);
      assertThat(redis.get(key)).isEqualTo("\u0000\u0000\u0000\u0000\u0000my setrange");
   }

   @Test
   public void testGetexWithEX() throws InterruptedException {
      RedisCommands<String, String> redis = redisConnection.sync();
      GetExArgs args = GetExArgs.Builder.ex(1);
      String key = "getexex";
      String value = "getex-value";
      redis.set(key, value);
      redis.getex(key, args);
      assertThat(redis.get(key)).isEqualTo(value);
      ((ControlledTimeService) this.timeService).advance(2000);
      assertThat(redis.get(key)).isNull();
   }

   @Test
   public void testGetexWithPEX() throws InterruptedException {
      RedisCommands<String, String> redis = redisConnection.sync();
      GetExArgs args = GetExArgs.Builder.px(500);
      String key = "getexpex";
      String value = "getexpex-value";
      redis.set(key, value);
      redis.getex(key, args);
      assertThat(redis.get(key)).isEqualTo(value);
      ((ControlledTimeService) this.timeService).advance(1000);
      assertThat(redis.get(key)).isNull();
   }

   @Test
   public void testGetexWithPERSIST() throws InterruptedException {
      RedisCommands<String, String> redis = redisConnection.sync();
      GetExArgs args = GetExArgs.Builder.ex(2);
      String key = "getexpersist";
      String value = "getexpersist-value";
      redis.set(key, value);
      redis.getex(key, args);
      assertThat(redis.get(key)).isEqualTo(value);
      ((ControlledTimeService) this.timeService).advance(1000);
      args = GetExArgs.Builder.persist();
      redis.getex(key, args);
      ((ControlledTimeService) this.timeService).advance(1500);
      assertThat(redis.get(key)).isEqualTo(value);
   }

   @Test
   public void testGetexWithEXNotPresent() throws InterruptedException {
      RedisCommands<String, String> redis = redisConnection.sync();
      GetExArgs args = GetExArgs.Builder.ex(2);
      String key = "getexex";
      redis.getex(key, args);
      assertThat(redis.get(key)).isNull();
   }

   @Test
   public void testSetWithEX() throws InterruptedException {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "setex";
      String value = "getex-value";
      var args = new SetArgs().ex(1);
      redis.set(key, value, args);
      assertThat(redis.get(key)).isEqualTo(value);
      ((ControlledTimeService) this.timeService).advance(2000);
      assertThat(redis.get(key)).isNull();
   }

   @Test
   public void testSetWithXX() throws InterruptedException {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "setxx";
      String value = "getex-value";
      var args = new SetArgs().xx();
      assertThat(redis.set(key, value, args)).isNull();
      redis.set(key, value);
      assertThat(redis.set(key, value, args)).isEqualTo("OK");
   }

   @Test
   public void testSetWithXXWithTTL() throws InterruptedException {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "setxx";
      String value = "getex-value";
      redis.set(key, value);
      var args = new SetArgs().xx().px(10000);
      assertThat(redis.set(key, value, args)).isEqualTo("OK");
      assertThat(redis.ttl(key)).isEqualTo(10);
   }

   @Test
   public void testMsetnx() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Map<String, String> values = new HashMap<String,String>();
      values.put("k1", "v1");
      values.put("k3", "v3");
      values.put("k4", "v4");
      assertThat(redis.msetnx(values)).isEqualTo(true);

      List<KeyValue<String, String>> expected = new ArrayList<>(4);
      expected.add(KeyValue.just("k1", "v1"));
      expected.add(KeyValue.empty("k2"));
      expected.add(KeyValue.just("k3", "v3"));
      expected.add(KeyValue.just("k4", "v4"));

      List<KeyValue<String, String>> results = redis.mget("k1", "k2", "k3", "k4");
      assertThat(results).containsExactlyElementsOf(expected);

      values.clear();
      values.put("k4", "v4");
      values.put("k5", "v5");
      values.put("k6", "v6");
      assertThat(redis.msetnx(values)).isEqualTo(false);

      expected.clear();
      expected.add(KeyValue.just("k4", "v4"));
      expected.add(KeyValue.empty("k5"));
      expected.add(KeyValue.empty("k6"));

      results = redis.mget("k4", "k5", "k6");
      assertThat(results).containsExactlyElementsOf(expected);
   }

   @Test
   public void testMsetnxSameKey() {
      // Needs custom command to allow byte[] args
      CustomStringCommands commands = CustomStringCommands.instance(redisConnection);
      Long l = commands.msetnxSameKey(new byte[]{'k','1'}, new byte[]{'v','1'}, new byte[]{'v','2'}, new byte[]{'v','3'}, new byte[]{'v','4'});
      assertThat(l).isEqualTo(1);
      String actual = redisConnection.sync().get("k1");
      assertThat(actual).isEqualTo("v4");
   }

   @Test
   public void testSetex() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThatThrownBy(() -> redis.setex("key", -30, "value"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR invalid expire time in 'SETEX' command");

      assertThat(redisConnection.isOpen()).isTrue();

      assertThat(redis.setex("key", 1, "value")).isEqualTo("OK");
      assertThat(redis.ttl("key")).isEqualTo(1);

      ((ControlledTimeService) timeService).advance(2_000);
      assertThat(redis.get("key")).isNull();
   }

   @Test
   public void testSetnx() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.setnx("key", "value")).isTrue();
      assertThat(redis.setnx("key", "another-value")).isFalse();

      assertThat(redis.get("key")).isEqualTo("value");
   }

   @Test
   public void testGetset() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.getset("key", "value")).isNull();
      assertThat(redis.getset("key", "another")).isEqualTo("value");
      assertThat(redis.get("key")).isEqualTo("another");
   }

   @Test
   public void testGetsetWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertWrongType(() -> redis.lpush("key","value"), () -> redis.getset("key", "shouldfail"));
      assertWrongType(() -> {} , () -> redis.get("key"));
      assertThat(redis.lrange("key", 0, -1)).containsExactly("value");
   }

   @Test
   public void testGetsetCounter() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.incr("counter")).isEqualTo(1);
      assertThat(redis.getset("counter", "0")).isEqualTo("1");
      assertThat(redis.get("counter")).isEqualTo("0");
   }

   @Test
   public void testPsetex() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.psetex("key", 1000, "value")).isEqualTo("OK");
      assertThat(redis.pttl("key")).isEqualTo(1000);

      ((ControlledTimeService) timeService).advance(1001);
      assertThat(redis.get("key")).isNull();
   }

   @Test
   public void testMget() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set("k1", "v1");
      redis.sadd("k2", "s1", "s2", "s3");
      redis.set("k3", "v3");
      redis.set("k4", "v4");
      var results = redis.mget("k1", "k2", "k3", "k4","k5");
      List<KeyValue<String, String>> expected = new ArrayList<>(5);
      expected.add(KeyValue.just("k1", "v1"));
      expected.add(KeyValue.empty("k2"));
      expected.add(KeyValue.just("k3", "v3"));
      expected.add(KeyValue.just("k4", "v4"));
      expected.add(KeyValue.empty("k5"));
      assertThat(results).containsExactlyElementsOf(expected);
   }

   @Test
   public void testDigest() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Test with existing key
      redis.set("digest-key", "Hello world");
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8).addKey("digest-key");
      String result = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(result).isNotNull();
      assertThat(result).hasSize(16); // 64-bit hash = 16 hex characters
      assertThat(result).matches("[0-9a-f]{16}");

      // Test with non-existing key
      args = new CommandArgs<>(StringCodec.UTF8).addKey("nonexistent-key");
      result = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(result).isNull();

      // Test same value produces same hash
      redis.set("digest-key2", "Hello world");
      args = new CommandArgs<>(StringCodec.UTF8).addKey("digest-key2");
      String result2 = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), args);
      args = new CommandArgs<>(StringCodec.UTF8).addKey("digest-key");
      String result1 = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(result1).isEqualTo(result2);

      // Test different values produce different hashes
      redis.set("digest-key3", "Different value");
      args = new CommandArgs<>(StringCodec.UTF8).addKey("digest-key3");
      String result3 = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(result3).isNotEqualTo(result1);
   }

   @Test
   public void testDelex() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Test unconditional delete
      redis.set("delex-key1", "value1");
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8).addKey("delex-key1");
      Long result = redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo(1L);
      assertThat(redis.get("delex-key1")).isNull();

      // Test delete non-existing key
      args = new CommandArgs<>(StringCodec.UTF8).addKey("nonexistent");
      result = redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo(0L);
   }

   @Test
   public void testDelexIfeq() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Test IFEQ - value matches
      redis.set("delex-ifeq", "matchme");
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-ifeq").add("IFEQ").add("matchme");
      Long result = redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo(1L);
      assertThat(redis.get("delex-ifeq")).isNull();

      // Test IFEQ - value doesn't match
      redis.set("delex-ifeq2", "actualvalue");
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-ifeq2").add("IFEQ").add("differentvalue");
      result = redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo(0L);
      assertThat(redis.get("delex-ifeq2")).isEqualTo("actualvalue");
   }

   @Test
   public void testDelexIfne() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Test IFNE - value doesn't match (should delete)
      redis.set("delex-ifne", "currentvalue");
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-ifne").add("IFNE").add("othervalue");
      Long result = redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo(1L);
      assertThat(redis.get("delex-ifne")).isNull();

      // Test IFNE - value matches (should not delete)
      redis.set("delex-ifne2", "samevalue");
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-ifne2").add("IFNE").add("samevalue");
      result = redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo(0L);
      assertThat(redis.get("delex-ifne2")).isEqualTo("samevalue");
   }

   @Test
   public void testDelexIfdeq() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // First get the digest of a value
      redis.set("delex-ifdeq", "Hello world");
      CommandArgs<String, String> digestArgs = new CommandArgs<>(StringCodec.UTF8).addKey("delex-ifdeq");
      String digest = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), digestArgs);

      // Test IFDEQ - digest matches (should delete)
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-ifdeq").add("IFDEQ").add(digest);
      Long result = redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo(1L);
      assertThat(redis.get("delex-ifdeq")).isNull();

      // Test IFDEQ - digest doesn't match (should not delete)
      redis.set("delex-ifdeq2", "Different value");
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-ifdeq2").add("IFDEQ").add(digest);
      result = redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo(0L);
      assertThat(redis.get("delex-ifdeq2")).isEqualTo("Different value");
   }

   @Test
   public void testDelexIfdne() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // First get the digest of a value
      redis.set("delex-ifdne", "Hello world");
      CommandArgs<String, String> digestArgs = new CommandArgs<>(StringCodec.UTF8).addKey("delex-ifdne");
      String digest = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), digestArgs);

      // Test IFDNE - digest matches (should NOT delete)
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-ifdne").add("IFDNE").add(digest);
      Long result = redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo(0L);
      assertThat(redis.get("delex-ifdne")).isEqualTo("Hello world");

      // Test IFDNE - digest doesn't match (should delete)
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-ifdne").add("IFDNE").add("0000000000000000");
      result = redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo(1L);
      assertThat(redis.get("delex-ifdne")).isNull();
   }

   @Test
   public void testSetIfeq() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Test IFEQ - value matches, should set
      redis.set("set-ifeq", "oldvalue");
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifeq").add("newvalue").add("IFEQ").add("oldvalue");
      String result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("OK");
      assertThat(redis.get("set-ifeq")).isEqualTo("newvalue");

      // Test IFEQ - value doesn't match, should not set
      redis.set("set-ifeq2", "actualvalue");
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifeq2").add("newvalue").add("IFEQ").add("wrongvalue");
      result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isNull();
      assertThat(redis.get("set-ifeq2")).isEqualTo("actualvalue");

      // Test IFEQ - key doesn't exist, should not create
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifeq-nonexistent").add("newvalue").add("IFEQ").add("anyvalue");
      result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isNull();
      assertThat(redis.get("set-ifeq-nonexistent")).isNull();
   }

   @Test
   public void testSetIfne() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Test IFNE - value doesn't match, should set
      redis.set("set-ifne", "currentvalue");
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifne").add("newvalue").add("IFNE").add("othervalue");
      String result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("OK");
      assertThat(redis.get("set-ifne")).isEqualTo("newvalue");

      // Test IFNE - value matches, should not set
      redis.set("set-ifne2", "samevalue");
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifne2").add("newvalue").add("IFNE").add("samevalue");
      result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isNull();
      assertThat(redis.get("set-ifne2")).isEqualTo("samevalue");

      // Test IFNE - key doesn't exist, should create
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifne-new").add("newvalue").add("IFNE").add("anyvalue");
      result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("OK");
      assertThat(redis.get("set-ifne-new")).isEqualTo("newvalue");
   }

   @Test
   public void testSetIfdeq() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // First get the digest of a value
      redis.set("set-ifdeq", "Hello world");
      CommandArgs<String, String> digestArgs = new CommandArgs<>(StringCodec.UTF8).addKey("set-ifdeq");
      String digest = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), digestArgs);

      // Test IFDEQ - digest matches, should set
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifdeq").add("newvalue").add("IFDEQ").add(digest);
      String result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("OK");
      assertThat(redis.get("set-ifdeq")).isEqualTo("newvalue");

      // Test IFDEQ - digest doesn't match, should not set
      redis.set("set-ifdeq2", "Different value");
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifdeq2").add("newvalue").add("IFDEQ").add(digest);
      result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isNull();
      assertThat(redis.get("set-ifdeq2")).isEqualTo("Different value");

      // Test IFDEQ - key doesn't exist, should not create
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifdeq-nonexistent").add("newvalue").add("IFDEQ").add(digest);
      result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isNull();
      assertThat(redis.get("set-ifdeq-nonexistent")).isNull();
   }

   @Test
   public void testSetIfdne() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // First get the digest of a value
      redis.set("set-ifdne", "Hello world");
      CommandArgs<String, String> digestArgs = new CommandArgs<>(StringCodec.UTF8).addKey("set-ifdne");
      String digest = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), digestArgs);

      // Test IFDNE - digest matches, should NOT set
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifdne").add("newvalue").add("IFDNE").add(digest);
      String result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isNull();
      assertThat(redis.get("set-ifdne")).isEqualTo("Hello world");

      // Test IFDNE - digest doesn't match, should set
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifdne").add("newvalue").add("IFDNE").add("0000000000000000");
      result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("OK");
      assertThat(redis.get("set-ifdne")).isEqualTo("newvalue");

      // Test IFDNE - key doesn't exist, should create
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifdne-new").add("newvalue").add("IFDNE").add(digest);
      result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("OK");
      assertThat(redis.get("set-ifdne-new")).isEqualTo("newvalue");
   }

   @Test
   public void testSetIfeqWithGet() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Test IFEQ with GET - value matches, should set and return old value
      redis.set("set-ifeq-get", "oldvalue");
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifeq-get").add("newvalue").add("IFEQ").add("oldvalue").add("GET");
      String result = redis.dispatch(new SimpleCommand("SET"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("oldvalue");
      assertThat(redis.get("set-ifeq-get")).isEqualTo("newvalue");

      // Test IFEQ with GET - value doesn't match, should not set but still return old value
      redis.set("set-ifeq-get2", "actualvalue");
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifeq-get2").add("newvalue").add("IFEQ").add("wrongvalue").add("GET");
      result = redis.dispatch(new SimpleCommand("SET"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("actualvalue");
      assertThat(redis.get("set-ifeq-get2")).isEqualTo("actualvalue");
   }

   // ======== DIGEST additional tests ========

   @Test
   public void testDigestEmptyString() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set("digest-empty", "");
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8).addKey("digest-empty");
      String result = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(result).isNotNull().hasSize(16).matches("[0-9a-f]{16}");
   }

   @Test
   public void testDigestVeryLongString() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String longStr = "Lorem ipsum dolor sit amet. ".repeat(1000);
      redis.set("digest-long", longStr);
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8).addKey("digest-long");
      String result = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(result).isNotNull().hasSize(16).matches("[0-9a-f]{16}");
   }

   @Test
   public void testDigestConsistencyAcrossSetOperations() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set("digest-cons", "original");
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8).addKey("digest-cons");
      String digest1 = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), args);

      redis.set("digest-cons", "changed");
      args = new CommandArgs<>(StringCodec.UTF8).addKey("digest-cons");
      String digest2 = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(digest1).isNotEqualTo(digest2);

      redis.set("digest-cons", "original");
      args = new CommandArgs<>(StringCodec.UTF8).addKey("digest-cons");
      String digest3 = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(digest1).isEqualTo(digest3);
   }

   @Test
   public void testDigestAlways16HexChars() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // Verify the digest always returns exactly 16 hex characters (including leading zeros)
      for (int i = 0; i < 100; i++) {
         redis.set("digest-lz", "test-value-" + i);
         CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8).addKey("digest-lz");
         String result = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), args);
         assertThat(result).hasSize(16).matches("[0-9a-f]{16}");
      }
   }

   // ======== DELEX additional tests ========

   @Test
   public void testDelexNonExistingKeyWithConditions() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.del("delex-nokey");

      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-nokey").add("IFEQ").add("hello");
      assertThat(redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args)).isEqualTo(0L);

      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-nokey").add("IFNE").add("hello");
      assertThat(redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args)).isEqualTo(0L);

      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-nokey").add("IFDEQ").add("0000000000000000");
      assertThat(redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args)).isEqualTo(0L);

      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-nokey").add("IFDNE").add("0000000000000000");
      assertThat(redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args)).isEqualTo(0L);
   }

   @Test
   public void testDelexEmptyStringIfeq() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.set("delex-empty", "");
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-empty").add("IFEQ").add("");
      assertThat(redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args)).isEqualTo(1L);
      assertThat(redis.exists("delex-empty")).isEqualTo(0);

      redis.set("delex-empty2", "");
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-empty2").add("IFEQ").add("notempty");
      assertThat(redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args)).isEqualTo(0L);
      assertThat(redis.exists("delex-empty2")).isEqualTo(1);
   }

   @Test
   public void testDelexDigestConsistencySameContent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.set("delex-dc1", "identical");
      redis.set("delex-dc2", "identical");

      CommandArgs<String, String> digestArgs = new CommandArgs<>(StringCodec.UTF8).addKey("delex-dc1");
      String digest1 = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), digestArgs);
      digestArgs = new CommandArgs<>(StringCodec.UTF8).addKey("delex-dc2");
      String digest2 = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), digestArgs);
      assertThat(digest1).isEqualTo(digest2);

      // Cross-key: use digest of key2 to delete key1 and vice versa
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-dc1").add("IFDEQ").add(digest2);
      assertThat(redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args)).isEqualTo(1L);

      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-dc2").add("IFDEQ").add(digest1);
      assertThat(redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args)).isEqualTo(1L);
   }

   @Test
   public void testDelexDigestDifferentContent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.set("delex-dd1", "value1");
      redis.set("delex-dd2", "value2");

      CommandArgs<String, String> digestArgs = new CommandArgs<>(StringCodec.UTF8).addKey("delex-dd1");
      String digest1 = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), digestArgs);
      digestArgs = new CommandArgs<>(StringCodec.UTF8).addKey("delex-dd2");
      String digest2 = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), digestArgs);
      assertThat(digest1).isNotEqualTo(digest2);

      // Wrong digest should not delete
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-dd1").add("IFDEQ").add(digest2);
      assertThat(redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args)).isEqualTo(0L);
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-dd2").add("IFDEQ").add(digest1);
      assertThat(redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args)).isEqualTo(0L);

      // Correct digest should delete
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-dd1").add("IFDEQ").add(digest1);
      assertThat(redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args)).isEqualTo(1L);
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-dd2").add("IFDEQ").add(digest2);
      assertThat(redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args)).isEqualTo(1L);
   }

   // ======== SET + GET combination tests ========

   @Test
   public void testSetIfeqWithGetKeyMissing() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.del("set-ifeq-get-miss");

      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifeq-get-miss").add("newvalue").add("IFEQ").add("hello").add("GET");
      String result = redis.dispatch(new SimpleCommand("SET"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(result).isNull();
      assertThat(redis.exists("set-ifeq-get-miss")).isEqualTo(0);
   }

   @Test
   public void testSetIfneWithGet() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // IFNE + GET - value doesn't match, should set, return old value
      redis.set("set-ifne-get1", "hello");
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifne-get1").add("world").add("IFNE").add("different").add("GET");
      String result = redis.dispatch(new SimpleCommand("SET"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("hello");
      assertThat(redis.get("set-ifne-get1")).isEqualTo("world");

      // IFNE + GET - value matches, should not set, return old value
      redis.set("set-ifne-get2", "hello");
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifne-get2").add("world").add("IFNE").add("hello").add("GET");
      result = redis.dispatch(new SimpleCommand("SET"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("hello");
      assertThat(redis.get("set-ifne-get2")).isEqualTo("hello");

      // IFNE + GET - key doesn't exist, should create, return nil
      redis.del("set-ifne-get3");
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifne-get3").add("world").add("IFNE").add("hello").add("GET");
      result = redis.dispatch(new SimpleCommand("SET"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(result).isNull();
      assertThat(redis.get("set-ifne-get3")).isEqualTo("world");
   }

   @Test
   public void testSetIfdeqWithGet() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // IFDEQ + GET - digest matches, should set, return old value
      redis.set("set-ifdeq-get1", "hello");
      CommandArgs<String, String> digestArgs = new CommandArgs<>(StringCodec.UTF8).addKey("set-ifdeq-get1");
      String digest = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), digestArgs);

      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifdeq-get1").add("world").add("IFDEQ").add(digest).add("GET");
      String result = redis.dispatch(new SimpleCommand("SET"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("hello");
      assertThat(redis.get("set-ifdeq-get1")).isEqualTo("world");

      // IFDEQ + GET - digest doesn't match, should not set, return old value
      redis.set("set-ifdeq-get2", "hello");
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifdeq-get2").add("world").add("IFDEQ").add("0000000000000000").add("GET");
      result = redis.dispatch(new SimpleCommand("SET"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("hello");
      assertThat(redis.get("set-ifdeq-get2")).isEqualTo("hello");

      // IFDEQ + GET - key doesn't exist, should not create, return nil
      redis.del("set-ifdeq-get3");
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifdeq-get3").add("world").add("IFDEQ").add(digest).add("GET");
      result = redis.dispatch(new SimpleCommand("SET"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(result).isNull();
      assertThat(redis.exists("set-ifdeq-get3")).isEqualTo(0);
   }

   @Test
   public void testSetIfdneWithGet() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // IFDNE + GET - digest doesn't match, should set, return old value
      redis.set("set-ifdne-get1", "hello");
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifdne-get1").add("world").add("IFDNE").add("0000000000000000").add("GET");
      String result = redis.dispatch(new SimpleCommand("SET"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("hello");
      assertThat(redis.get("set-ifdne-get1")).isEqualTo("world");

      // IFDNE + GET - digest matches, should not set, return old value
      redis.set("set-ifdne-get2", "hello");
      CommandArgs<String, String> digestArgs = new CommandArgs<>(StringCodec.UTF8).addKey("set-ifdne-get2");
      String digest = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), digestArgs);
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifdne-get2").add("world").add("IFDNE").add(digest).add("GET");
      result = redis.dispatch(new SimpleCommand("SET"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("hello");
      assertThat(redis.get("set-ifdne-get2")).isEqualTo("hello");

      // IFDNE + GET - key doesn't exist, should create, return nil
      redis.del("set-ifdne-get3");
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifdne-get3").add("world").add("IFDNE").add("0000000000000000").add("GET");
      result = redis.dispatch(new SimpleCommand("SET"), new ValueOutput<>(StringCodec.UTF8), args);
      assertThat(result).isNull();
      assertThat(redis.get("set-ifdne-get3")).isEqualTo("world");
   }

   // ======== SET + expiration tests ========

   @Test
   public void testSetIfeqWithExpiration() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.set("set-ifeq-ex", "hello");
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifeq-ex").add("world").add("IFEQ").add("hello").add("EX").add("10");
      String result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("OK");
      assertThat(redis.get("set-ifeq-ex")).isEqualTo("world");
      assertThat(redis.ttl("set-ifeq-ex")).isEqualTo(10);
   }

   @Test
   public void testSetIfneWithExpiration() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.set("set-ifne-ex", "hello");
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifne-ex").add("world").add("IFNE").add("different").add("EX").add("10");
      String result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("OK");
      assertThat(redis.get("set-ifne-ex")).isEqualTo("world");
      assertThat(redis.ttl("set-ifne-ex")).isEqualTo(10);
   }

   @Test
   public void testSetIfdeqWithExpiration() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.set("set-ifdeq-ex", "hello");
      CommandArgs<String, String> digestArgs = new CommandArgs<>(StringCodec.UTF8).addKey("set-ifdeq-ex");
      String digest = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), digestArgs);

      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifdeq-ex").add("world").add("IFDEQ").add(digest).add("EX").add("10");
      String result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("OK");
      assertThat(redis.get("set-ifdeq-ex")).isEqualTo("world");
      assertThat(redis.ttl("set-ifdeq-ex")).isEqualTo(10);
   }

   @Test
   public void testSetIfdneWithExpiration() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.set("set-ifdne-ex", "hello");
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifdne-ex").add("world").add("IFDNE").add("0000000000000000").add("EX").add("10");
      String result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("OK");
      assertThat(redis.get("set-ifdne-ex")).isEqualTo("world");
      assertThat(redis.ttl("set-ifdne-ex")).isEqualTo(10);
   }

   // ======== SET edge case tests ========

   @Test
   public void testSetIfeqEmptyString() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.set("set-ifeq-empty", "");
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifeq-empty").add("world").add("IFEQ").add("");
      String result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("OK");
      assertThat(redis.get("set-ifeq-empty")).isEqualTo("world");
   }

   @Test
   public void testSetIfneEmptyString() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.set("set-ifne-empty", "");
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-ifne-empty").add("world").add("IFNE").add("");
      String result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isNull();
      assertThat(redis.get("set-ifne-empty")).isEqualTo("");
   }

   @Test
   public void testSetConditionCaseInsensitive() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.set("set-case", "hello");
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-case").add("world1").add("ifeq").add("hello");
      String result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("OK");
      assertThat(redis.get("set-case")).isEqualTo("world1");
   }

   @Test
   public void testSetDigestUppercaseHex() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // IFDEQ should accept uppercase hex digest
      redis.set("set-upper", "hello");
      CommandArgs<String, String> digestArgs = new CommandArgs<>(StringCodec.UTF8).addKey("set-upper");
      String digest = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), digestArgs);
      String upperDigest = digest.toUpperCase();

      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-upper").add("world").add("IFDEQ").add(upperDigest);
      String result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isEqualTo("OK");
      assertThat(redis.get("set-upper")).isEqualTo("world");

      // IFDNE with matching uppercase digest should NOT set
      redis.set("set-upper2", "hello");
      digestArgs = new CommandArgs<>(StringCodec.UTF8).addKey("set-upper2");
      digest = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), digestArgs);
      upperDigest = digest.toUpperCase();

      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-upper2").add("world").add("IFDNE").add(upperDigest);
      result = redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args);
      assertThat(result).isNull();
      assertThat(redis.get("set-upper2")).isEqualTo("hello");
   }

   @Test
   public void testSetDigestConsistencyCrossKey() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.set("set-dc1", "identical");
      redis.set("set-dc2", "identical");

      CommandArgs<String, String> digestArgs = new CommandArgs<>(StringCodec.UTF8).addKey("set-dc1");
      String digest1 = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), digestArgs);
      digestArgs = new CommandArgs<>(StringCodec.UTF8).addKey("set-dc2");
      String digest2 = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), digestArgs);
      assertThat(digest1).isEqualTo(digest2);

      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-dc1").add("new1").add("IFDEQ").add(digest1);
      assertThat(redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args)).isEqualTo("OK");
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-dc2").add("new2").add("IFDEQ").add(digest2);
      assertThat(redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args)).isEqualTo("OK");

      assertThat(redis.get("set-dc1")).isEqualTo("new1");
      assertThat(redis.get("set-dc2")).isEqualTo("new2");
   }

   @Test
   public void testSetDigestDifferentContent() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.set("set-dd1", "value1");
      redis.set("set-dd2", "value2");

      CommandArgs<String, String> digestArgs = new CommandArgs<>(StringCodec.UTF8).addKey("set-dd1");
      String digest1 = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), digestArgs);
      digestArgs = new CommandArgs<>(StringCodec.UTF8).addKey("set-dd2");
      String digest2 = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), digestArgs);
      assertThat(digest1).isNotEqualTo(digest2);

      // Wrong digest should not set
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-dd1").add("new1").add("IFDEQ").add(digest2);
      assertThat(redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args)).isNull();
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-dd2").add("new2").add("IFDEQ").add(digest1);
      assertThat(redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args)).isNull();
      assertThat(redis.get("set-dd1")).isEqualTo("value1");
      assertThat(redis.get("set-dd2")).isEqualTo("value2");

      // Correct digest should set
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-dd1").add("new1").add("IFDEQ").add(digest1);
      assertThat(redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args)).isEqualTo("OK");
      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("set-dd2").add("new2").add("IFDEQ").add(digest2);
      assertThat(redis.dispatch(new SimpleCommand("SET"), new io.lettuce.core.output.StatusOutput<>(StringCodec.UTF8), args)).isEqualTo("OK");
      assertThat(redis.get("set-dd1")).isEqualTo("new1");
      assertThat(redis.get("set-dd2")).isEqualTo("new2");
   }

   @Test
   public void testDelexUppercaseHex() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.set("delex-upper", "hello");
      CommandArgs<String, String> digestArgs = new CommandArgs<>(StringCodec.UTF8).addKey("delex-upper");
      String digest = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), digestArgs);
      String upperDigest = digest.toUpperCase();

      // IFDEQ with uppercase hex should match
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-upper").add("IFDEQ").add(upperDigest);
      assertThat(redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args)).isEqualTo(1L);
      assertThat(redis.exists("delex-upper")).isEqualTo(0);

      // IFDNE with uppercase matching digest should NOT delete
      redis.set("delex-upper2", "hello");
      digestArgs = new CommandArgs<>(StringCodec.UTF8).addKey("delex-upper2");
      digest = redis.dispatch(new SimpleCommand("DIGEST"), new ValueOutput<>(StringCodec.UTF8), digestArgs);
      upperDigest = digest.toUpperCase();

      args = new CommandArgs<>(StringCodec.UTF8)
            .addKey("delex-upper2").add("IFDNE").add(upperDigest);
      assertThat(redis.dispatch(new SimpleCommand("DELEX"), new io.lettuce.core.output.IntegerOutput<>(StringCodec.UTF8), args)).isEqualTo(0L);
      assertThat(redis.exists("delex-upper2")).isEqualTo(1);
   }

   private static class SimpleCommand implements ProtocolKeyword {
      private final String name;

      SimpleCommand(String name) {
         this.name = name;
      }

      @Override
      public byte[] getBytes() {
         return name.getBytes(StandardCharsets.UTF_8);
      }

      @Override
      public String name() {
         return name;
      }
   }
}
