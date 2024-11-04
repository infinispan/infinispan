package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.List;

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
   public void testScriptExists() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String sha = redis.scriptLoad("""
            return redis.call('auth', KEYS[1], ARGV[1])
            """);
      List<Boolean> exists = redis.scriptExists(sha, "0");
      assertThat(exists).containsExactly(true, false);
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
   public void testReturnMap() {
      RedisCommands<String, String> redis = redisConnection.sync();
      List<String> out = redis.eval("""
            redis.call('sadd', KEYS[1], ARGV[1])
            redis.call('sadd', KEYS[1], ARGV[2])
            return redis.call('smembers', KEYS[1], 0, 1)
            """, ScriptOutputType.MULTI, new String[]{k()}, v(0), v(1));
      assertThat(out).containsExactly(v(1), v(0));
   }

   @Test
   public void testFcall() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String lib = redis.functionLoad("""
            #!lua name=mylib
            redis.register_function('myfunc', function(keys, args) return args[1] end)
            """);
      assertEquals("mylib", lib);
      String out = redis.fcall("myfunc", ScriptOutputType.VALUE, "hello");
      assertEquals("hello", out);
   }
}
