package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "server.resp.ScriptingCommandsTest")
public class ScriptingCommandsTest extends SingleNodeRespBaseTest {

   @Override
   public Object[] factory() {
      return new Object[]{
            new ScriptingCommandsTest(),
            new ScriptingCommandsTest().withAuthorization()
      };
   }

   @Test
   public void testEval() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String out = redis.eval("""
            return redis.call('set', KEYS[1], ARGV[1])
            """, ScriptOutputType.STATUS, new String[]{k()}, v());
      assertEquals("OK", out);
      assertThat(redis.get(k())).isEqualTo(v());
   }

   @Test
   public void testEvalRo() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThatThrownBy(() ->
            redis.evalReadOnly("""
                  return redis.call('set', KEYS[1], ARGV[1])
                  """.getBytes(StandardCharsets.US_ASCII), ScriptOutputType.STATUS, new String[]{k(0)}, v(0)))
            .hasMessageContaining("ERR Write commands are not allowed from read-only scripts.");
   }

   @Test
   public void testEvalSha() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String sha = redis.scriptLoad("""
            return redis.call('set', KEYS[1], ARGV[1])
            """);
      String out = redis.evalsha(sha, ScriptOutputType.STATUS, new String[]{k()}, v());
      assertEquals("OK", out);
      assertThat(redis.get(k())).isEqualTo(v());
   }

   @Test
   public void testEvalShaRo() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String sha = redis.scriptLoad("""
            return redis.call('set', KEYS[1], ARGV[1])
            """);
      assertThatThrownBy(() ->
            redis.evalshaReadOnly(sha, ScriptOutputType.STATUS, new String[]{k(0)}, v(0)))
            .hasMessageContaining("ERR Write commands are not allowed from read-only scripts.");
   }

   @Test
   public void testNotAllowed() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String sha = redis.scriptLoad("""
            return redis.call('auth', KEYS[1], ARGV[1])
            """);
      assertThatThrownBy(() ->
            redis.evalshaReadOnly(sha, ScriptOutputType.STATUS, new String[]{k(0)}, v(0)))
            .hasMessageContaining("ERR This Redis command is not allowed from script");
   }

   @Test
   public void testOsLibraryOnlyExposesOsClock() {
      // The os library is available but only exposes os.clock() - all other operations are not available
      RedisCommands<String, String> redis = redisConnection.sync();

      // os.clock() should be available and return a number
      String clockResult = redis.eval("return tostring(os.clock())", ScriptOutputType.VALUE);
      assertThat(clockResult).isNotNull();
      assertThat(clockResult).matches("\\d+\\.?\\d*"); // Should be a number like "0.123" or "123"

      // Verify os.clock is actually a function
      String clockType = redis.eval("return type(os.clock)", ScriptOutputType.VALUE);
      assertThat(clockType).isEqualTo("function");

      // os.execute() should NOT be available
      assertThatThrownBy(() ->
            redis.eval("return os.execute('id > /tmp/pwn')", ScriptOutputType.STATUS))
            .hasMessageContaining("attempt to call")
            .hasMessageContaining("nil value");

      // os.exit() should NOT be available
      assertThatThrownBy(() ->
            redis.eval("return os.exit()", ScriptOutputType.STATUS))
            .hasMessageContaining("attempt to call")
            .hasMessageContaining("nil value");

      // os.getenv() should NOT be available
      assertThatThrownBy(() ->
            redis.eval("return os.getenv('PATH')", ScriptOutputType.STATUS))
            .hasMessageContaining("attempt to call")
            .hasMessageContaining("nil value");

      // os.remove() should NOT be available
      assertThatThrownBy(() ->
            redis.eval("return os.remove('/tmp/test')", ScriptOutputType.STATUS))
            .hasMessageContaining("attempt to call")
            .hasMessageContaining("nil value");

      // os.rename() should NOT be available
      assertThatThrownBy(() ->
            redis.eval("return os.rename('/tmp/a', '/tmp/b')", ScriptOutputType.STATUS))
            .hasMessageContaining("attempt to call")
            .hasMessageContaining("nil value");
   }

   @Test
   public void testDangerousLibrariesAreNotAvailable() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // os is available (but only with os.clock), so we only check io and package
      List<Object> out = redis.eval("""
            local r = {} for _, n in ipairs({'io', 'package'}) do
               local ok = pcall(function() return _G[n] end) table.insert(r, (not ok) and 1 or 0)
            end; return {r[1], r[2]}
            """, ScriptOutputType.MULTI);
      assertThat(out).containsExactly(1L, 1L);

      // Verify os IS available by checking its type
      String osType = redis.eval("return type(os)", ScriptOutputType.VALUE);
      assertThat(osType).isEqualTo("table");
   }

   @Test
   public void testMissingGlobalRaisesError() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThatThrownBy(() ->
            redis.eval("return definitely_missing_global_xyz", ScriptOutputType.STATUS))
            .hasMessageContaining("Script attempted to access nonexistent global variable 'definitely_missing_global_xyz'");
   }

   @Test
   public void testGlobalsProtectionIsRestoredBetweenScripts() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // A malicious script strips the _G protection and plants a global for other users of this context to find.
      // Stripping is only effective within its own execution: the protection is re-installed before every subsequent script.
      assertEquals("stripped", redis.eval("""
            setmetatable(_G, nil)
            planted_secret_xyz = 42
            return 'stripped'
            """, ScriptOutputType.STATUS));
      // The pool may serve a stripped context on any following invocation; probe enough times to cover every slot.
      for (int i = 0; i < 8; i++) {
         assertThatThrownBy(() -> redis.eval("return planted_secret_xyz", ScriptOutputType.STATUS))
               .hasMessageContaining("Script attempted to access nonexistent global variable 'planted_secret_xyz'");
      }
   }

   @Test
   public void testCorruptedGlobalsAreRestoredBetweenScripts() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // A script can overwrite existing (allow-listed) globals; their pristine values are restored before every subsequent script.
      assertEquals("corrupted", redis.eval("""
            tostring = nil; error = 99; return 'corrupted'
            """, ScriptOutputType.STATUS));
      for (int i = 0; i < 8; i++) {
         List<Object> out = redis.eval("return {type(tostring), type(error)}", ScriptOutputType.MULTI);
         assertThat(out).containsExactly("function", "function");
      }
   }


   @Test
   public void testCallError() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String sha = redis.scriptLoad("""
            return redis.call('sintercard', '0', 'a')
            """);
      assertThatThrownBy(() ->
            redis.evalshaReadOnly(sha, ScriptOutputType.STATUS, new String[]{k(0)}, v(0)))
            .hasMessageContaining("ERR numkeys should be greater than 0");
   }

   @Test
   public void testPCallError() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String sha = redis.scriptLoad("""
            return redis.pcall('sintercard', '0', 'a')
            """);
      assertThatThrownBy(() ->
            redis.evalshaReadOnly(sha, ScriptOutputType.STATUS, new String[]{k(0)}, v(0)))
            .hasMessageContaining("ERR numkeys should be greater than 0");
   }

   @Test
   public void testReturnNumber() {
      RedisCommands<String, String> redis = redisConnection.sync();
      long out = redis.eval("""
            redis.call('lpush', KEYS[1], ARGV[1])
            redis.call('lpush', KEYS[1], ARGV[2])
            return redis.call('llen', KEYS[1])
            """, ScriptOutputType.INTEGER, new String[]{k()}, v(0), v(1));
      assertEquals(2, out);
   }

   @Test
   public void testReturnBoolean() {
      RedisCommands<String, String> redis = redisConnection.sync();
      boolean out = redis.eval("""
            redis.call('sadd', KEYS[1], ARGV[1])
            return redis.call('SISMEMBER', KEYS[1], ARGV[1])
            """, ScriptOutputType.BOOLEAN, new String[]{k()}, v(0));
      assertTrue(out);
   }

   @Test
   public void testReturnList() {
      RedisCommands<String, String> redis = redisConnection.sync();
      List<String> out = redis.eval("""
            redis.call('lpush', KEYS[1], ARGV[1])
            redis.call('lpush', KEYS[1], ARGV[2])
            return redis.call('lrange', KEYS[1], 0, 1)
            """, ScriptOutputType.MULTI, new String[]{k()}, v(0), v(1));
      assertThat(out).containsExactly(v(1), v(0));
   }

   @Test
   public void testReturnMap() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Map<String, String> map = redis.eval("""
            return redis.call('config', 'get', 'appendonly')
            """, ScriptOutputType.OBJECT, new String[0], new String[0]);
      assertThat(map).containsExactlyEntriesOf(Map.of("appendonly", "no"));
   }

   @Test
   public void testReturnSet() {
      RedisCommands<String, String> redis = redisConnection.sync();
      List<String> out = redis.eval("""
            redis.call('sadd', KEYS[1], ARGV[1])
            redis.call('sadd', KEYS[1], ARGV[2])
            return redis.call('smembers', KEYS[1])
            """, ScriptOutputType.MULTI, new String[]{k()}, v(0), v(1));
      assertThat(out).containsExactly(v(1), v(0));
   }

   @Test
   public void testReturnZSet() {
      RedisCommands<String, String> redis = redisConnection.sync();
      List<List<Object>> out = redis.eval("""
            redis.call('zadd', KEYS[1], 1, ARGV[1])
            redis.call('zadd', KEYS[1], 1, ARGV[2])
            redis.call('zadd', KEYS[1], 2, ARGV[3], 3, ARGV[4])
            return redis.call('zrange', KEYS[1], 0, -1, 'withscores')
            """, ScriptOutputType.MULTI, new String[]{k()}, v(0), v(1), v(2), v(3));
      assertThat(out).containsExactly(List.of(v(0), 1L), List.of(v(1), 1L), List.of(v(2), 2L), List.of(v(3), 3L));
   }

   @Test
   public void testMultiBulkTypeConversion() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.del(k());
      redis.rpush(k(), v(0));
      redis.rpush(k(), v(1));
      redis.rpush(k(), v(2));
      List<Object> out = redis.eval("""
            local foo = redis.pcall('lrange', KEYS[1], 0, -1)
            return {type(foo),foo[1],foo[2],foo[3],# foo}
            """, ScriptOutputType.MULTI, k());
      assertThat(out).containsExactly("table", v(0), v(1), v(2), 3L);
   }

   @Test
   public void testErrorReplyTypeConversion() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), v());
      List<Object> out = redis.eval("""
            local foo = redis.pcall('incr',KEYS[1])
                        return {type(foo),foo['err']}
            """, ScriptOutputType.MULTI, k());
      assertThat(out).containsExactly("table", "ERR value is not an integer or out of range");
   }

   @Test
   public void testScriptDoesNotBlockOnBlpop() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.rpush(k(), "1");
      redis.rpop(k());
      Object out = redis.eval("""
            return redis.pcall('blpop',KEYS[1],0)
            """, ScriptOutputType.VALUE, k());
      assertThat(out).isNull();
   }

   @Test
   public void testNonDeterministicCommands() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String out = redis.eval("""
            redis.pcall('randomkey'); return redis.pcall('set','x','ciao')
            """, ScriptOutputType.STATUS);
      assertEquals("OK", out);
   }

   @Test
   public void testEnginePRNGCanBeSeededCorrectly() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String rand1 = redis.eval("""
            math.randomseed(ARGV[1]); return tostring(math.random())
            """, ScriptOutputType.VALUE, new String[0], Integer.toString(10));
      String rand2 = redis.eval("""
            math.randomseed(ARGV[1]); return tostring(math.random())
            """, ScriptOutputType.VALUE, new String[0], Integer.toString(10));
      String rand3 = redis.eval("""
            math.randomseed(ARGV[1]); return tostring(math.random())
            """, ScriptOutputType.VALUE, new String[0], Integer.toString(20));
      assertThat(rand1).isEqualTo(rand2);
      assertThat(rand2).isNotEqualTo(rand3);
   }

   @Test
   public void testScriptExists() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String sha = redis.scriptLoad("""
            return "Descartes"
            """);
      List<Boolean> exists = redis.scriptExists(sha, "0");
      assertThat(exists).containsExactly(true, false);
   }

   @Test
   public void testScriptFlush() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String sha = redis.scriptLoad("""
            return "testScriptFlush"
            """);
      String out = redis.evalsha(sha, ScriptOutputType.VALUE, new String[0]);
      assertEquals("testScriptFlush", out);
      redis.scriptFlush();
      assertThatThrownBy(() -> redis.evalsha(sha, ScriptOutputType.VALUE, new String[0])).hasMessage("ERR NOSCRIPT No matching script. Please use EVAL.");
   }

   @Test
   public void testReturn_G() {
      RedisCommands<String, String> redis = redisConnection.sync();
      List<?> list = redis.eval("""
            return _G
            """, ScriptOutputType.OBJECT);
      assertThat(list).isEmpty();
   }

   @Test
   public void testReturnTableWithMetatableThatRaisesError() {
      RedisCommands<String, String> redis = redisConnection.sync();
      List<?> list = redis.eval("""
            local a = {};
            setmetatable(a,{__index=function() foo() end})
            return a
            """, ScriptOutputType.OBJECT);
      assertThat(list).isEmpty();
   }

   @Test
   public void testReturnTableWithMetatableThatCallsResp() {
      RedisCommands<String, String> redis = redisConnection.sync();
      List<?> list = redis.eval("""
            local a = {};
            setmetatable(a,{__index=function() redis.call('set', 'x', '1') end})
            return a
            """, ScriptOutputType.OBJECT, new String[]{"x"});
      assertThat(list).isEmpty();
      String x = redis.get("x");
      assertNull(x);
   }


   @Test
   public void testReturnLuaInteger() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Long l = redis.eval("""
            return 100.5
            """, ScriptOutputType.INTEGER);
      assertEquals(100, l.intValue());
   }

   @Test
   public void testReturnLuaString() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String s = redis.eval("""
            return 'hello world'
            """, ScriptOutputType.VALUE);
      assertEquals("hello world", s);
   }

   @Test
   public void testReturnLuaBoolean() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Boolean b = redis.eval("""
            return true
            """, ScriptOutputType.BOOLEAN);
      assertTrue(b);
      b = redis.eval("""
            return false
            """, ScriptOutputType.BOOLEAN);
      assertFalse(b);
   }

   @Test
   public void testReturnLuaStatusCodeReply() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String s = redis.eval("""
            return {ok='fine'}
            """, ScriptOutputType.STATUS);
      assertEquals("fine", s);
   }

   @Test
   public void testReturnLuaErrorReply() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThatThrownBy(() -> redis.eval("""
            return {err='ERR this is an error'}
            """, ScriptOutputType.STATUS)).hasMessage("ERR this is an error");
   }

   @Test
   public void testReturnLuaTable() {
      RedisCommands<String, String> redis = redisConnection.sync();
      List<Object> list = redis.eval("""
            return {1,2,3,'ciao',{1,2}}
            """, ScriptOutputType.OBJECT);
      assertThat(list).containsExactly(1L, 2L, 3L, "ciao", List.of(1L, 2L));
   }

   @Test
   public void testInvalidSHA1() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThatThrownBy(() -> redis.evalsha("NotValidShaSUM", ScriptOutputType.OBJECT)).hasMessageContaining("NOSCRIPT");
   }

   @Test
   public void testNonDefinedSHA1() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThatThrownBy(() -> redis.evalsha("ffd632c7d33e571e9f24556ebed26c3479a87130", ScriptOutputType.OBJECT)).hasMessageContaining("NOSCRIPT");
   }
}
