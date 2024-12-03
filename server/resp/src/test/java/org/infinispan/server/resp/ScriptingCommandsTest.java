package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;

@Test(groups = "functional", testName = "server.resp.ScriptingCommandsTest")
public class ScriptingCommandsTest extends SingleNodeRespBaseTest {
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
            return redis.call('smembers', KEYS[1], 0, 1)
            """, ScriptOutputType.MULTI, new String[]{k()}, v(0), v(1));
      assertThat(out).containsExactly(v(1), v(0));
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
}
