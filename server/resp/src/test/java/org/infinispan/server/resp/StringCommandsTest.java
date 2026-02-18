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

@Test(groups = "functional", testName = "server.resp.StringCommandsTest")
public class StringCommandsTest extends SingleNodeRespBaseTest {

   @Override
   public Object[] factory() {
      return new Object[]{
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

   @Test
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

   @Test
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
      int[][] idx = Arrays.stream(idxs).filter(pos -> pos.length == 1 || pos[1] - pos[0] >= minLen - 1)
            .toArray(int[][]::new);
      StringMatchResult res = redis.stralgoLcs(args);
      checkIdx(resp, idx, res, false);
   }

   @DataProvider
   public Object[][] lcsCases() {
      return new Object[][]{
            {"GAC", "AGCAT", "AC", new int[][]{{2, 2, 2, 2}, {1, 1, 0, 0}, {2}}},
            {"XMJYAUZ", "MZJAWXU", "MJAU",
                  new int[][]{{5, 5, 6, 6}, {4, 4, 3, 3}, {2, 2, 2, 2}, {1, 1, 0, 0}, {4}}},
            {"ohmytext", "mynewtext", "mytext", new int[][]{{4, 7, 5, 8}, {2, 3, 0, 1}, {6}}},
            {"ABCBDAB", "BDCABA", "BDAB", new int[][]{{5, 6, 3, 4}, {3, 4, 0, 1}, {4}}},
            {"ABCEZ12 21AAZ", "12ABZ 21AZAZ", "ABZ 21AAZ",
                  new int[][]{{11, 12, 10, 11}, {7, 10, 5, 8}, {4, 4, 4, 4}, {0, 1, 2, 3}, {9}}}
      };
   }

   @DataProvider
   public Object[][] lcsCasesWithMinLen() {
      List<Object[]> testCases = new ArrayList<>();
      var minLengths = new Object[][]{{1}, {2}, {4}, {10}};
      var lcsCases = this.lcsCases();
      for (Object[] len : minLengths) {
         for (Object[] lcsCase : lcsCases) {
            testCases.add(Stream.concat(Arrays.stream(lcsCase), Arrays.stream(len)).toArray());
         }
      }
      return testCases.toArray(new Object[0][]);
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
      Map<String, String> values = new HashMap<String, String>();
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
      Long l = commands.msetnxSameKey(new byte[]{'k', '1'}, new byte[]{'v', '1'}, new byte[]{'v', '2'}, new byte[]{'v', '3'}, new byte[]{'v', '4'});
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
      assertWrongType(() -> redis.lpush("key", "value"), () -> redis.getset("key", "shouldfail"));
      assertWrongType(() -> {
      }, () -> redis.get("key"));
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
      var results = redis.mget("k1", "k2", "k3", "k4", "k5");
      List<KeyValue<String, String>> expected = new ArrayList<>(5);
      expected.add(KeyValue.just("k1", "v1"));
      expected.add(KeyValue.empty("k2"));
      expected.add(KeyValue.just("k3", "v3"));
      expected.add(KeyValue.just("k4", "v4"));
      expected.add(KeyValue.empty("k5"));
      assertThat(results).containsExactlyElementsOf(expected);
   }

   @Test
   public void testSetAndGetEmptyItem() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "empty-item";
      redis.set(key, "");
      assertThat(redis.get(key)).isEqualTo("");
      assertThat(redis.strlen(key)).isEqualTo(0);
   }

   @Test
   public void testSetNxOption() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "set-nx";

      // NX on non-existing key should succeed
      assertThat(redis.set(key, "value", new SetArgs().nx())).isEqualTo("OK");
      assertThat(redis.get(key)).isEqualTo("value");

      // NX on existing key should fail
      assertThat(redis.set(key, "new-value", new SetArgs().nx())).isNull();
      assertThat(redis.get(key)).isEqualTo("value");
   }

   @Test
   public void testSetGetOption() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "set-get";

      // SET with GET on non-existing key returns nil
      assertThat(redis.setGet(key, "value")).isNull();

      // SET with GET on existing key returns old value
      assertThat(redis.setGet(key, "new-value")).isEqualTo("value");
      assertThat(redis.get(key)).isEqualTo("new-value");
   }

   @Test
   public void testSetGetWithXX() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "set-get-xx";

      // SET GET XX on non-existing key returns nil and doesn't set
      assertThat(redis.setGet(key, "value", new SetArgs().xx())).isNull();
      assertThat(redis.get(key)).isNull();

      // Create the key, then SET GET XX should return old value
      redis.set(key, "first");
      assertThat(redis.setGet(key, "second", new SetArgs().xx())).isEqualTo("first");
      assertThat(redis.get(key)).isEqualTo("second");
   }

   @Test
   public void testSetGetWithNX() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "set-get-nx";

      // SET GET NX on non-existing key should set and return nil
      assertThat(redis.setGet(key, "value", new SetArgs().nx())).isNull();
      assertThat(redis.get(key)).isEqualTo("value");

      // SET GET NX on existing key should not set and return old value
      assertThat(redis.setGet(key, "new-value", new SetArgs().nx())).isEqualTo("value");
      assertThat(redis.get(key)).isEqualTo("value");
   }

   @Test
   public void testSetGetWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "set-get-wrongtype";
      redis.lpush(key, "listvalue");

      assertThatThrownBy(() -> redis.setGet(key, "value"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");
   }

   @Test
   public void testSetWithPX() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "set-px";
      redis.set(key, "value", new SetArgs().px(500));
      assertThat(redis.get(key)).isEqualTo("value");
      ((ControlledTimeService) this.timeService).advance(1000);
      assertThat(redis.get(key)).isNull();
   }

   @Test
   public void testSetWithEXAT() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "set-exat";
      // Set with EXAT = current time + 2 seconds
      long exat = this.timeService.wallClockTime() / 1000 + 2;
      redis.set(key, "value", new SetArgs().exAt(exat));
      assertThat(redis.get(key)).isEqualTo("value");
      ((ControlledTimeService) this.timeService).advance(3000);
      assertThat(redis.get(key)).isNull();
   }

   @Test
   public void testSetWithPXAT() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "set-pxat";
      // Set with PXAT = current time + 2000 milliseconds
      long pxat = this.timeService.wallClockTime() + 2000;
      redis.set(key, "value", new SetArgs().pxAt(pxat));
      assertThat(redis.get(key)).isEqualTo("value");
      ((ControlledTimeService) this.timeService).advance(3000);
      assertThat(redis.get(key)).isNull();
   }

   @Test
   public void testGetexWithEXAT() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "getex-exat";
      redis.set(key, "value");
      long exat = this.timeService.wallClockTime() / 1000 + 2;
      redis.getex(key, GetExArgs.Builder.exAt(exat));
      assertThat(redis.get(key)).isEqualTo("value");
      ((ControlledTimeService) this.timeService).advance(3000);
      assertThat(redis.get(key)).isNull();
   }

   @Test
   public void testGetexWithPXAT() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "getex-pxat";
      redis.set(key, "value");
      long pxat = this.timeService.wallClockTime() + 2000;
      redis.getex(key, GetExArgs.Builder.pxAt(pxat));
      assertThat(redis.get(key)).isEqualTo("value");
      ((ControlledTimeService) this.timeService).advance(3000);
      assertThat(redis.get(key)).isNull();
   }

   @Test
   public void testGetexNoOption() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "getex-nooption";
      redis.set(key, "value");

      // GETEX with no option should return value without modifying TTL
      assertThat(redis.getex(key, new GetExArgs())).isEqualTo("value");
      assertThat(redis.get(key)).isEqualTo("value");
   }

   @Test
   public void testSetrangeWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "setrange-wrongtype";
      redis.lpush(key, "listvalue");

      assertThatThrownBy(() -> redis.setrange(key, 0, "value"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");
   }

   @Test
   public void testGetrangeWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "getrange-wrongtype";
      redis.lpush(key, "listvalue");

      assertThatThrownBy(() -> redis.getrange(key, 0, -1))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");
   }

   @Test
   public void testSubstr() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "substr-test";
      redis.set(key, "Hello, World!");
      // SUBSTR is equivalent to GETRANGE
      assertThat(redis.getrange(key, 0, 4)).isEqualTo("Hello");
      assertThat(redis.getrange(key, 7, 11)).isEqualTo("World");
      // Verify SUBSTR command directly via dispatch
      io.lettuce.core.protocol.ProtocolKeyword substrCmd = new io.lettuce.core.protocol.ProtocolKeyword() {
         @Override
         public byte[] getBytes() {
            return "SUBSTR".getBytes();
         }

         @Override
         public String name() {
            return "SUBSTR";
         }
      };
      String result = redis.dispatch(substrCmd,
            new io.lettuce.core.output.StatusOutput<>(io.lettuce.core.codec.StringCodec.UTF8),
            new io.lettuce.core.protocol.CommandArgs<>(io.lettuce.core.codec.StringCodec.UTF8)
                  .addKey(key).add(0).add(4));
      assertThat(result).isEqualTo("Hello");
   }

   @Test
   public void testStrlenIntegerEncoded() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "strlen-integer";
      redis.set(key, "12345");
      assertThat(redis.strlen(key)).isEqualTo(5);
      redis.set(key, "-1");
      assertThat(redis.strlen(key)).isEqualTo(2);
   }

   @Test
   public void testSetnxAgainstExpiredKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "setnx-expired";

      // Set with TTL
      redis.setex(key, 1, "old-value");
      assertThat(redis.get(key)).isEqualTo("old-value");

      // Wait for expiration
      ((ControlledTimeService) this.timeService).advance(2000);
      assertThat(redis.get(key)).isNull();

      // SETNX on expired key should succeed
      assertThat(redis.setnx(key, "new-value")).isTrue();
      assertThat(redis.get(key)).isEqualTo("new-value");
   }

   @Test
   public void testSetnxAgainstNotExpiredVolatileKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "setnx-volatile";

      redis.setex(key, 100, "old-value");
      assertThat(redis.setnx(key, "new-value")).isFalse();
      assertThat(redis.get(key)).isEqualTo("old-value");
   }

   @Test
   public void testMsetBaseCase() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Map<String, String> values = new HashMap<>();
      values.put("mset-k1", "v1");
      values.put("mset-k2", "v2");
      values.put("mset-k3", "v3");
      assertThat(redis.mset(values)).isEqualTo("OK");

      assertThat(redis.get("mset-k1")).isEqualTo("v1");
      assertThat(redis.get("mset-k2")).isEqualTo("v2");
      assertThat(redis.get("mset-k3")).isEqualTo("v3");
   }

   @Test
   public void testMsetSameKeyTwice() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // When same key appears twice, last value wins
      redis.dispatch(io.lettuce.core.protocol.CommandType.MSET,
            new io.lettuce.core.output.StatusOutput<>(io.lettuce.core.codec.StringCodec.UTF8),
            new io.lettuce.core.protocol.CommandArgs<>(io.lettuce.core.codec.StringCodec.UTF8)
                  .addKey("mset-dup").addValue("first")
                  .addKey("mset-dup").addValue("second"));
      assertThat(redis.get("mset-dup")).isEqualTo("second");
   }

   @Test
   public void testMsetOverwritesExisting() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set("mset-ow", "old");
      Map<String, String> values = new HashMap<>();
      values.put("mset-ow", "new");
      redis.mset(values);
      assertThat(redis.get("mset-ow")).isEqualTo("new");
   }

   @Test
   public void testMgetNonExistingKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set("mget-k1", "v1");
      List<KeyValue<String, String>> results = redis.mget("mget-k1", "mget-nonexist", "mget-k1");
      assertThat(results).hasSize(3);
      assertThat(results.get(0).getValue()).isEqualTo("v1");
      assertThat(results.get(1).hasValue()).isFalse();
      assertThat(results.get(2).getValue()).isEqualTo("v1");
   }

   @Test
   public void testMgetAgainstNonStringKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set("mget-str", "v1");
      redis.lpush("mget-list", "item");
      // MGET returns empty for non-string keys
      List<KeyValue<String, String>> results = redis.mget("mget-str", "mget-list");
      assertThat(results).hasSize(2);
      assertThat(results.get(0).getValue()).isEqualTo("v1");
      assertThat(results.get(1).hasValue()).isFalse();
   }

   @Test
   public void testGetdelWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "getdel-wrongtype";
      assertWrongType(() -> redis.lpush(key, "list"), () -> redis.getdel(key));
   }

   @Test
   public void testSetrangeIntegerEncodedKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "setrange-int";
      redis.set(key, "12345");
      assertThat(redis.setrange(key, 1, "XX")).isEqualTo(5);
      assertThat(redis.get(key)).isEqualTo("1XX45");
   }

   @Test
   public void testGetrangeIntegerEncodedValue() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "getrange-int";
      redis.set(key, "12345");
      assertThat(redis.getrange(key, 0, 2)).isEqualTo("123");
      assertThat(redis.getrange(key, -3, -1)).isEqualTo("345");
   }

   @Test
   public void testSetMultipleOptionsAtOnce() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "set-multi-opts";
      // SET with NX and EX
      assertThat(redis.set(key, "value", new SetArgs().nx().ex(100))).isEqualTo("OK");
      assertThat(redis.get(key)).isEqualTo("value");
      assertThat(redis.ttl(key)).isEqualTo(100);

      // SET with XX and PX (update existing)
      assertThat(redis.set(key, "new-value", new SetArgs().xx().px(50000))).isEqualTo("OK");
      assertThat(redis.get(key)).isEqualTo("new-value");
      assertThat(redis.pttl(key)).isGreaterThan(0);
   }

   @Test
   public void testAppendCreatesKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "append-creates";
      // APPEND on non-existing key creates it
      assertThat(redis.append(key, "hello")).isEqualTo(5);
      assertThat(redis.get(key)).isEqualTo("hello");
   }

   @Test
   public void testAppendIntToRaw() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "append-int2raw";
      redis.set(key, "123");
      redis.append(key, "abc");
      assertThat(redis.get(key)).isEqualTo("123abc");
      assertThat(redis.strlen(key)).isEqualTo(6);
   }

   @Test
   public void testIncrOnLargeValue() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "incr-large";
      redis.set(key, "999999999999");
      assertThat(redis.incr(key)).isEqualTo(1000000000000L);
   }

   @Test
   public void testDecrOnLargeValue() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "decr-large";
      redis.set(key, "-999999999999");
      assertThat(redis.decr(key)).isEqualTo(-1000000000000L);
   }

   @Test
   public void testIncrWithSpacesInValue() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "incr-spaces";
      redis.set(key, " 10 ");
      assertThatThrownBy(() -> redis.incr(key))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("value is not an integer or out of range");
   }

   @Test
   public void testStrlenWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "strlen-wrongtype";
      assertWrongType(() -> redis.lpush(key, "list"), () -> redis.strlen(key));
   }

   @Test
   public void testAppendWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "append-wrongtype";
      assertWrongType(() -> redis.lpush(key, "list"), () -> redis.append(key, "val"));
   }

   @Test
   public void testIncrWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "incr-wrongtype";
      assertWrongType(() -> redis.lpush(key, "list"), () -> redis.incr(key));
   }

   @Test
   public void testDecrWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "decr-wrongtype";
      assertWrongType(() -> redis.lpush(key, "list"), () -> redis.decr(key));
   }

   @Test
   public void testIncrbyWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "incrby-wrongtype";
      assertWrongType(() -> redis.lpush(key, "list"), () -> redis.incrby(key, 5));
   }

   @Test
   public void testDecrbyWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "decrby-wrongtype";
      assertWrongType(() -> redis.lpush(key, "list"), () -> redis.decrby(key, 5));
   }

   @Test
   public void testIncrbyFloatWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "incrbyfloat-wrongtype";
      assertWrongType(() -> redis.lpush(key, "list"), () -> redis.incrbyfloat(key, 1.5));
   }

   @Test
   public void testSetAndGetLargePayload() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "large-payload";
      // 1MB payload
      char[] chars = new char[1024 * 1024];
      Arrays.fill(chars, 'x');
      String largeValue = new String(chars);
      redis.set(key, largeValue);
      assertThat(redis.get(key)).isEqualTo(largeValue);
      assertThat(redis.strlen(key)).isEqualTo(1024 * 1024);
   }

   @Test
   public void testGetexOnExpiredKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "getex-expired";
      redis.setex(key, 1, "value");
      ((ControlledTimeService) this.timeService).advance(2000);
      // GETEX on expired key returns nil
      assertThat(redis.getex(key, new GetExArgs())).isNull();
   }

   @Test
   public void testSetGetWithExpiration() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "setget-expire";
      redis.set(key, "old-value");

      // SET GET with EX returns old value and sets new TTL
      assertThat(redis.setGet(key, "new-value", new SetArgs().ex(100))).isEqualTo("old-value");
      assertThat(redis.get(key)).isEqualTo("new-value");
      assertThat(redis.ttl(key)).isEqualTo(100);
   }

   @Test
   public void testIncrbyFloatScientificNotation() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "incrbyfloat-sci";
      redis.set(key, "10");
      // Redis supports scientific notation for INCRBYFLOAT
      Double result = redis.incrbyfloat(key, 1.0e2);
      assertThat(result).isEqualTo(110.0);
   }

   @Test
   public void testGetNonExistingKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.get("totally-nonexistent")).isNull();
   }

   @Test
   public void testSetWrongType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "set-wrongtype";
      redis.lpush(key, "item");
      // Infinispan returns WRONGTYPE for SET on non-string keys
      assertThatThrownBy(() -> redis.set(key, "value"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("WRONGTYPE");
   }

   @Test
   public void testMsetnxWithExistentKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set("msetnx-exists", "existing");

      Map<String, String> values = new HashMap<>();
      values.put("msetnx-exists", "new");
      values.put("msetnx-new", "value");
      // MSETNX fails atomically - neither key should be set
      assertThat(redis.msetnx(values)).isFalse();
      assertThat(redis.get("msetnx-exists")).isEqualTo("existing");
      assertThat(redis.get("msetnx-new")).isNull();
   }

   @Test
   public void testDecrbyNegativeAmount() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "decrby-neg";
      redis.set(key, "10");
      // DECRBY with negative = effectively increment
      assertThat(redis.decrby(key, -5)).isEqualTo(15);
   }

   @Test
   public void testIncrbyNegativeAmount() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "incrby-neg";
      redis.set(key, "10");
      // INCRBY with negative = effectively decrement
      assertThat(redis.incrby(key, -3)).isEqualTo(7);
   }

   @Test
   public void testSetrangeEmptyValueOnNonExisting() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "setrange-empty-ne";
      // SETRANGE with empty value on non-existing key doesn't create it
      assertThat(redis.setrange(key, 0, "")).isEqualTo(0);
      assertThat(redis.exists(key)).isEqualTo(0);
   }

   @Test
   public void testGetrangeNonExistingKey() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.getrange("nonexist-getrange", 0, -1)).isEqualTo("");
   }
}
