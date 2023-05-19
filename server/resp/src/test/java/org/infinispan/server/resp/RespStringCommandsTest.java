package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.withPrecision;

import java.util.Arrays;
import java.util.stream.Stream;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.StrAlgoArgs;
import io.lettuce.core.StringMatchResult;
import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "server.resp.RespStringCommandsTest")
public class RespStringCommandsTest extends SingleNodeRespBaseTest {
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
        redis.set(key, "A long string for testing");
        assertThat(redis.getrange(key, 1, 7)).isEqualTo(" long s");
        // Check negative range
        assertThat(redis.getrange(key, 10, -2)).isEqualTo("ing for testin");
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
    void testSetrange() {
       RedisCommands<String, String> redis = redisConnection.sync();
       String key = "setrange";
       redis.set(key, "A long string for testing");
       assertThat(redis.setrange(key, 2, "tiny")).isEqualTo(25);
       assertThat(redis.get(key)).isEqualTo("A tiny string for testing");
    }

    @Test
    void testSetrangePatchOverflowsLength() {
       RedisCommands<String, String> redis = redisConnection.sync();
       String key = "setrange";
       redis.set(key, "A long string for testing");
       assertThat(redis.setrange(key, 18, "setrange testing")).isEqualTo(34);
       assertThat(redis.get(key)).isEqualTo("A long string for setrange testing");
    }

    @Test
    void testSetrangeOffsetGreaterThanLength() {
       RedisCommands<String, String> redis = redisConnection.sync();
       String key = "setrange";
       redis.set(key, "A long string for testing");
       assertThat(redis.setrange(key, 30, "my setrange")).isEqualTo(41);
       assertThat(redis.get(key)).isEqualTo("A long string for testing\u0000\u0000\u0000\u0000\u0000my setrange");
    }

    @Test
    void testSetrangeNotPresent() {
       RedisCommands<String, String> redis = redisConnection.sync();
       String key = "setrange-notpresent";
       assertThat(redis.setrange(key, 5, "my setrange")).isEqualTo(16);
       assertThat(redis.get(key)).isEqualTo("\u0000\u0000\u0000\u0000\u0000my setrange");
    }
}
