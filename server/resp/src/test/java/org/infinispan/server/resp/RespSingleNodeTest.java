package org.infinispan.server.resp;

import static io.lettuce.core.ScoredValue.just;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.resp.test.RespTestingUtil.ADMIN;
import static org.infinispan.server.resp.test.RespTestingUtil.HOST;
import static org.infinispan.server.resp.test.RespTestingUtil.OK;
import static org.infinispan.server.resp.test.RespTestingUtil.PONG;
import static org.infinispan.server.resp.test.RespTestingUtil.assertWrongType;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.assertj.core.api.SoftAssertions;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.test.skip.SkipTestNG;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.util.Version;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.security.Security;
import org.infinispan.server.resp.commands.Commands;
import org.infinispan.server.resp.test.CommonRespTests;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.lettuce.core.ExpireArgs;
import io.lettuce.core.FlushMode;
import io.lettuce.core.KeyScanArgs;
import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisConnectionException;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.SetArgs;
import io.lettuce.core.SortArgs;
import io.lettuce.core.StrAlgoArgs;
import io.lettuce.core.StringMatchResult;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.json.DefaultJsonParser;
import io.lettuce.core.json.JsonPath;
import io.lettuce.core.json.JsonValue;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.output.StatusOutput;
import io.lettuce.core.protocol.CommandArgs;
import io.lettuce.core.protocol.CommandType;
import io.lettuce.core.protocol.ProtocolKeyword;

/**
 * Base class for single node tests.
 *
 * @author William Burns
 * @since 14.0
 */
@Test(groups = "functional", testName = "server.resp.RespSingleNodeTest")
public class RespSingleNodeTest extends SingleNodeRespBaseTest {

   private CacheMode cacheMode = CacheMode.LOCAL;
   private boolean simpleCache;

   @Override
   public Object[] factory() {
      return new Object[]{
            new RespSingleNodeTest(),
            new RespSingleNodeTest().withAuthorization(),
            new RespSingleNodeTest().simpleCache(),
            new RespSingleNodeTest().simpleCache().withAuthorization(),
      };
   }

   protected RespSingleNodeTest simpleCache() {
      this.cacheMode = CacheMode.LOCAL;
      this.simpleCache = true;
      return this;
   }

   @Override
   protected String parameters() {
      return super.parameters() + " -- [simpleCache=" + simpleCache + ", cacheMode=" + cacheMode + "]";
   }

   @Override
   protected void amendConfiguration(ConfigurationBuilder configurationBuilder) {
      if (simpleCache) {
         configurationBuilder.clustering().cacheMode(CacheMode.LOCAL).simpleCache(true);
      } else {
         configurationBuilder.clustering().cacheMode(cacheMode);
      }
   }

   @Test
   public void testSetMultipleOptions() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Should return (nil), failed since value does not exist
      SetArgs args = SetArgs.Builder.xx();
      assertThat(redis.set("key", "value", args)).isNull();

      // Should return OK, because value does not exist.
      args = SetArgs.Builder.nx();
      assertThat(redis.set("key", "value", args)).isEqualTo(OK);
      assertThat(redis.get("key")).isEqualTo("value");

      // Should return (nil), because value exists.
      assertThat(redis.set("key", "value3", args)).isNull();
      assertThat(redis.get("key")).isEqualTo("value");

      // Should return OK, value exists
      args = SetArgs.Builder.xx();
      assertThat(redis.set("key", "value2", args)).isEqualTo(OK);
      assertThat(redis.get("key")).isEqualTo("value2");

      // Should insert with TTL. This one returns the value.
      args = SetArgs.Builder.ex(60);
      assertThat(redis.setGet("key", "value3", args)).isEqualTo("value2");

      CacheEntry<Object, Object> entry = cache.getAdvancedCache()
            .withMediaType(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OCTET_STREAM)
            .getCacheEntry("key".getBytes(StandardCharsets.UTF_8));
      assertThat(new String((byte[]) entry.getValue())).isEqualTo("value3");
      assertThat(entry.getLifespan()).isEqualTo(60_000);

      // We insert while keeping the TTL.
      args = SetArgs.Builder.keepttl();
      assertThat(redis.set("key", "value4", args)).isEqualTo(OK);
      entry = cache.getAdvancedCache()
            .withMediaType(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OCTET_STREAM)
            .getCacheEntry("key".getBytes(StandardCharsets.UTF_8));
      assertThat(new String((byte[]) entry.getValue())).isEqualTo("value4");
      assertThat(entry.getLifespan()).isEqualTo(60_000);

      // Conditional operation keeping TTL.
      args = SetArgs.Builder.keepttl().xx();

      assertThat(redis.set("key", "value5", args)).isEqualTo(OK);
      entry = cache.getAdvancedCache()
            .withMediaType(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OCTET_STREAM)
            .getCacheEntry("key".getBytes(StandardCharsets.UTF_8));
      assertThat(new String((byte[]) entry.getValue())).isEqualTo("value5");
      assertThat(entry.getLifespan()).isEqualTo(60_000);

      // Key exist and keeping TTL, but conditional failing. Should return nil.
      args = SetArgs.Builder.keepttl().nx();
      String res = redis.set("key", "value5", args);
      assertThat(res).isNull();

      // No NPE when keeping TTL, key not exists, and conditional succeed.
      args = SetArgs.Builder.keepttl().nx();
      assertThat(redis.set("randomKey", "value", args)).isEqualTo(OK);

      // No NPE when keeping TTL and key doesn't exist.
      args = SetArgs.Builder.keepttl();
      assertThat(redis.set("otherKey", "value", args)).isEqualTo(OK);

      redis.del("key", "randomKey", "otherKey");
   }

   public void testConditionalSetOperationWithReturn() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = UUID.randomUUID().toString();

      // Should return (nil), failed since value does not exist.
      SetArgs args = SetArgs.Builder.xx();
      assertThat(redis.setGet(key, "something", args)).isNull();
      assertThat(redis.get(key)).isNull();
      // Should return (nil), because value does not exist, but operation succeeded.
      args = SetArgs.Builder.nx();
      String res = redis.setGet(key, "value", args);
      assertThat(res).isNull();
      assertThat(redis.get(key)).isEqualTo("value");

      // Should return the previous because value exists but operation failed.
      assertThat(redis.setGet(key, "value2", args)).isEqualTo("value");
      assertThat(redis.get(key)).isEqualTo("value");

      // Should return previous value but succeeded.
      args = SetArgs.Builder.xx();
      assertThat(redis.setGet(key, "value2", args)).isEqualTo("value");
      assertThat(redis.get(key)).isEqualTo("value2");
   }

   public void testSetMGet() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set("k1", "v1");
      redis.set("k3", "v3");
      redis.set("k4", "v4");

      List<KeyValue<String, String>> expected = new ArrayList<>(4);
      expected.add(KeyValue.just("k1", "v1"));
      expected.add(KeyValue.empty("k2"));
      expected.add(KeyValue.just("k3", "v3"));
      expected.add(KeyValue.just("k4", "v4"));

      List<KeyValue<String, String>> results = redis.mget("k1", "k2", "k3", "k4");
      assertThat(results).containsExactlyElementsOf(expected);
   }

   public void testSetEmptyStringMGet() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set("k1", "");
      redis.set("k3", "value2");

      assertThat(redis.get("k1")).isEmpty();

      List<KeyValue<String, String>> expected = new ArrayList<>(3);
      expected.add(KeyValue.just("k1", ""));
      expected.add(KeyValue.empty("k2"));
      expected.add(KeyValue.just("k3", "value2"));

      List<KeyValue<String, String>> results = redis.mget("k1", "k2", "k3");
      assertThat(results).containsExactlyElementsOf(expected);
   }

   public void testMSetMGet() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Map<String, String> values = new HashMap<>();
      values.put("k1", "v1");
      values.put("k3", "v3");
      values.put("k4", "v4");
      redis.mset(values);

      List<KeyValue<String, String>> expected = new ArrayList<>(4);
      expected.add(KeyValue.just("k1", "v1"));
      expected.add(KeyValue.empty("k2"));
      expected.add(KeyValue.just("k3", "v3"));
      expected.add(KeyValue.just("k4", "v4"));

      List<KeyValue<String, String>> results = redis.mget("k1", "k2", "k3", "k4");
      assertThat(results).containsExactlyElementsOf(expected);
   }

   public void testSetGetDelete() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set("k1", "v1");
      String v = redis.get("k1");
      assertThat(v).isEqualTo("v1");

      redis.del("k1");

      assertThat(redis.get("k1")).isNull();
      assertThat(redis.get("something")).isNull();
   }

   @Test
   public void testDelNonStrings() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // DEL non string
      redis.rpush("list1", "v1", "v2", "v3");
      long c = redis.del("list1");
      assertThat(c).isEqualTo(1);
      // DEL non string and string
      redis.sadd("set1", "v1", "v2", "v3");
      redis.set("string1", "v1");
      c = redis.del("set1", "string1");
      assertThat(c).isEqualTo(2);
      // DEL non string and non string
      redis.rpush("list1", "v1", "v2", "v3");
      redis.sadd("set1", "v1", "v2", "v3");
      c = redis.del("list1", "set1");
      assertThat(c).isEqualTo(2);
      // DEL non string and non existent
      redis.sadd("set1", "v1", "v2", "v3");
      c = redis.del("set1", "non-existent");
      assertThat(c).isEqualTo(1);
   }

   public void testSetGetBigValue() {
      RedisCommands<String, String> redis = redisConnection.sync();
      StringBuilder sb = new StringBuilder();
      String charsToChoose = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

      for (int i = 0; i < 10_000; ++i) {
         sb.append(charsToChoose.charAt(ThreadLocalRandom.current().nextInt(charsToChoose.length())));
      }
      String actualString = sb.toString();
      redis.set("k1", actualString);
      assertThat(redis.get("k1")).isEqualTo(actualString);
   }

   public void testPingNoArg() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.ping()).isEqualTo(PONG);
   }

   @Test
   public void testPingArg() {
      RedisCodec<String, String> codec = StringCodec.UTF8;
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.dispatch(CommandType.PING,
            new StatusOutput<>(codec),
            new CommandArgs<>(codec).add("Hey"))).isEqualTo("Hey");
   }

   public void testEcho() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String stringToSend = "HI THERE!";
      assertThat(redis.echo(stringToSend)).isEqualTo(stringToSend);
   }

   public void testCommand() {
      RedisCommands<String, String> redis = redisConnection.sync();

      List<Object> commands = redis.command();
      assertThat(commands.size()).isEqualTo(Commands.all().size());
   }

   public void testAuth() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Exceptions.expectException(RedisCommandExecutionException.class,
            "WRONGPASS invalid username-password pair or user is disabled.",
            () -> redis.auth("user", "pass"));
   }

   public void testNotImplementedCommand() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Exceptions.expectException(RedisCommandExecutionException.class, "ERR unknown command",
            () -> redis.xdel("not-supported", "should error"));
   }

   public void testPipeline() throws ExecutionException, InterruptedException, TimeoutException {
      CommonRespTests.testPipeline(redisConnection);
   }

   @Test
   public void testUpperLowercase() {
      RedisCommands<String, String> redis = redisConnection.sync();
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8).addValue("Hello");
      String response = redis.dispatch(new SimpleCommand("ECHO"), new StatusOutput<>(StringCodec.UTF8), args);
      assertEquals("Hello", response);
      response = redis.dispatch(new SimpleCommand("echo"), new StatusOutput<>(StringCodec.UTF8), args);
      assertEquals("Hello", response);
   }

   @Test
   public void testInfo() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String info = redis.info();
      assertThat(info).contains("# Server");
      assertThat(info).contains("# Client");
      assertThat(info).contains("# Modules");
      assertThat(info).contains("# Persistence");
      assertThat(info).contains("# Keyspace");
      info = redis.info("server");
      assertThat(info).contains("# Server");
      assertThat(info).doesNotContain("# Client");
   }

   @Test
   public void testModule() {
      RedisCommands<String, String> redis = redisConnection.sync();
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8).addValue("LIST");
      List<Object> response = redis.dispatch(new SimpleCommand("MODULE"), new ArrayOutput<>(StringCodec.UTF8), args);
      assertEquals(1, response.size());
   }

   @Test
   public void testDbSize() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Long size = redis.dbsize();
      assertThat(size).isEqualTo(cache.size());
      redis.set("dbsize-key", "dbsize-value");
      assertThat(redis.dbsize()).isEqualTo(size + 1);
   }

   @Test
   public void testClient() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Long l = redis.clientId();
      assertThat(l).isNotNull();
      String list = redis.clientList();
      assertThat(list).contains("id=" + l);
      assertThat(list).contains("name= ");
      redis.clientSetname("test");
      assertThat(redis.clientGetname()).isEqualTo("test");
      list = redis.clientList();
      assertThat(list).contains("name=test");

      redis.clientSetinfo("lib-ver", "15.0");
      redis.clientSetinfo("lib-name", "Infinispan-RESP");
      list = redis.clientList();
      assertThat(list)
            .contains("lib-ver=15.0")
            .contains("lib-name=Infinispan-RESP");

      assertThatThrownBy(() -> redis.clientSetinfo("lib-ver", "wrong version"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR lib-ver cannot contain spaces, newlines or special characters.");
      assertThatThrownBy(() -> redis.clientSetinfo("lib-name", "José"))
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessage("ERR lib-name cannot contain spaces, newlines or special characters.");
   }

   public static class SimpleCommand implements ProtocolKeyword {
      private final String name;

      public SimpleCommand(String name) {
         this.name = name;
      }

      @Override
      public byte[] getBytes() {
         return name.getBytes(StandardCharsets.UTF_8);
      }

      @Override
      public String toString() {
         return name;
      }
   }

   @Test
   public void testExists() {
      // Test on 10 entries
      RedisCommands<String, String> redis = redisConnection.sync();
      IntStream.range(0, 10).map(i -> 2 * i).forEach(i -> redis.set("key" + i, "value " + i));
      // Check 20 keys, 10 exist
      String[] keys = IntStream.range(0, 21).boxed().map(v -> "key" + v).toArray(String[]::new);
      assertThat(redis.exists(keys)).isEqualTo(10);
   }

   @Test
   public void testExistsMisc() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set("key1", "value1");
      redis.set("key2", "value2");
      redis.set("key3", "value3");
      assertThat(redis.exists("key1")).isEqualTo(1);
      assertThat(redis.exists("key1", "key2", "key3")).isEqualTo(3);
      assertThat(redis.exists("nonexistent-key")).isEqualTo(0);
      assertThat(redis.exists("key1", "nonexistent-key", "key2")).isEqualTo(2);
      assertThat(redis.exists("key1", "nonexistent-key", "key1")).isEqualTo(2);
      assertThat(redis.exists("nonexistent-key", "nonexistent-key", "key1")).isEqualTo(1);
   }

   @Test
   public void testFlushDb() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(0), v(0));
      assertThat(redis.dbsize()).isGreaterThan(0);
      redis.flushdb(FlushMode.SYNC);
      assertThat(redis.dbsize()).isEqualTo(0);
      redis.set(k(0), v(0));
      assertThat(redis.dbsize()).isGreaterThan(0);
      redis.flushdb(FlushMode.ASYNC);
      eventually(() -> redis.dbsize() == 0);
   }

   @Test
   public void testFlushAll() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(0), v(0));
      assertThat(redis.dbsize()).isGreaterThan(0);
      redis.flushall(FlushMode.SYNC);
      assertThat(redis.dbsize()).isEqualTo(0);
      redis.set(k(0), v(0));
      assertThat(redis.dbsize()).isGreaterThan(0);
      redis.flushall(FlushMode.ASYNC);
      eventually(() -> redis.dbsize() == 0);
   }

   @Test
   public void testScan() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.flushdb();
      Set<String> all = new HashSet<>();
      for (int i = 0; i < 15; i++) {
         String k = k(i);
         redis.set(k, v(i));
         all.add(k);
      }
      Set<String> keys = new HashSet<>();
      for (KeyScanCursor<String> cursor = redis.scan(); ; cursor = redis.scan(cursor)) {
         keys.addAll(cursor.getKeys());
         if (cursor.isFinished())
            break;
      }
      assertThat(keys).containsExactlyInAnyOrderElementsOf(all);
   }

   @Test
   public void testScanCount() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.flushdb();
      Set<String> all = new HashSet<>();
      for (int i = 0; i < 15; i++) {
         String k = k(i);
         redis.set(k, v(i));
         all.add(k);
      }
      assertScanWithCount(all, 5);
      assertScanWithCount(all, 14);
      assertScanWithCount(all, Integer.MAX_VALUE - 100);
   }

   private void assertScanWithCount(Set<String> all, int count) {
      RedisCommands<String, String> redis = redisConnection.sync();
      Set<String> keys = new HashSet<>();
      ScanArgs args = ScanArgs.Builder.limit(count);
      for (KeyScanCursor<String> cursor = redis.scan(args); ; cursor = redis.scan(cursor, args)) {
         if (!cursor.isFinished()) {
            assertThat(cursor.getKeys()).hasSize(count);
         }
         keys.addAll(cursor.getKeys());
         if (cursor.isFinished())
            break;
      }
      assertThat(keys).hasSize(all.size());
      assertThat(keys).containsExactlyInAnyOrderElementsOf(all);
   }

   @Test
   public void testScanMatch() {
      // This only works with certain key types
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.flushdb();
      Set<String> all = new HashSet<>();
      for (int i = 0; i < 15; i++) {
         String k = k(i);
         redis.set(k, v(i));
         all.add(k);
      }
      Set<String> keys = new HashSet<>();
      ScanArgs args = ScanArgs.Builder.matches("k1*");
      for (KeyScanCursor<String> cursor = redis.scan(args); ; cursor = redis.scan(cursor, args)) {
         for (String key : cursor.getKeys()) {
            assertThat(key).startsWith("k1");
            keys.add(key);
         }
         if (cursor.isFinished())
            break;
      }
   }

   @Test
   public void testLargeScanMatch() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.set("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "value")).isEqualTo(OK);

      ScanArgs args = ScanArgs.Builder.matches("a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*a*b");
      KeyScanCursor<String> cursor = redis.scan(args);
      assertThat(cursor.getKeys()).isEmpty();
      assertThat(cursor.isFinished()).isTrue();
   }

   @Test
   public void testScanFilters() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Set<String> all = new HashSet<>();
      for (int i = 0; i < 15; i++) {
         String k = k(i);
         redis.set(k, v(i));
         all.add(k);
      }

      for (int i = 15; i < 30; i++) {
         String k = k(i);
         if (i < 20) {
            redis.sadd(k, v(i));
            continue;
         }

         if (i < 25) {
            redis.zadd(k, 1.2, v(i));
            continue;
         }

         redis.hset(k, Map.of(k, v(i)));
      }

      // First scan everything. We have different types mixed here.
      Set<String> keys = new HashSet<>();
      for (KeyScanCursor<String> cursor = redis.scan(); ; cursor = redis.scan(cursor)) {
         keys.addAll(cursor.getKeys());
         if (cursor.isFinished())
            break;
      }

      assertThat(keys)
            .hasSize(30)
            .containsAll(all);

      keys.clear();

      // Now we scan only the strings.
      ScanArgs args = KeyScanArgs.Builder.type(RespTypes.string.name());
      for (KeyScanCursor<String> cursor = redis.scan(args); ; cursor = redis.scan(cursor, args)) {
         keys.addAll(cursor.getKeys());
         if (cursor.isFinished())
            break;
      }

      assertThat(keys)
            .hasSize(all.size())
            .containsExactlyInAnyOrderElementsOf(all);

      keys.clear();

      // Now we mix glob and type filter!
      args = KeyScanArgs.Builder
            .type(RespTypes.string.name())
            .match("k1*");
      for (KeyScanCursor<String> cursor = redis.scan(args); ; cursor = redis.scan(cursor, args)) {
         for (String key : cursor.getKeys()) {
            assertThat(key).startsWith("k1");
            keys.add(key);
         }
         if (cursor.isFinished())
            break;
      }

      assertThat(keys).hasSize(6);
   }

   @Test
   public void testClusterShardsSingleNode() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThatThrownBy(redis::clusterShards)
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR This instance has cluster support disabled");
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
      int[][] idx = Arrays
            .stream(idxs).filter(pos -> pos.length == 1 || pos[1] - pos[0] >= minLen - 1)
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
   public void testTTL() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), v());
      assertThat(redis.ttl(k())).isEqualTo(-1);
      assertThat(redis.ttl(k(1))).isEqualTo(-2);
      redis.set(k(2), v(2), SetArgs.Builder.ex(10_000));
      assertThat(redis.ttl(k(2))).isEqualTo(10_000L);

      timeService.advance(5_000, TimeUnit.SECONDS);

      assertThat(redis.ttl(k(2))).isEqualTo(5_000L);
   }

   @Test
   public void testPTTL() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), v());
      assertThat(redis.pttl(k())).isEqualTo(-1);
      assertThat(redis.pttl(k(1))).isEqualTo(-2);
      redis.set(k(2), v(2), SetArgs.Builder.ex(10_000));
      assertThat(redis.pttl(k(2))).isEqualTo(10_000_000L);
   }

   @Test
   public void testPTTLTypes() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.hset(k(), v(), v());
      assertThat(redis.pttl(k())).isEqualTo(-1);
      assertThat(redis.pttl(k(1))).isEqualTo(-2);
      redis.hset(k(2), v(2), v(2));
      redis.expire(k(2), 10);
      assertThat(redis.pttl(k(2))).isEqualTo(10_000L);

      timeService.advance(5_000);

      assertThat(redis.pttl(k(2))).isEqualTo(5_000L);
   }

   @Test
   public void testExpireTypes() {
      RedisCommands<String, String> redis = redisConnection.sync();

      redis.hset(k(1), v(1), v(1));
      assertThat(redis.expire(k(1), 1)).isTrue();

      redis.lpush(k(2), v(2));
      assertThat(redis.expire(k(2), 1)).isTrue();

      redis.zadd(k(3), 10, v(3));
      assertThat(redis.expire(k(3), 1)).isTrue();

      redis.pfadd(k(4), v(4));
      assertThat(redis.expire(k(4), 1)).isTrue();

      redis.sadd(k(5), v(5));
      assertThat(redis.expire(k(5), 1)).isTrue();

      redis.set(k(6), v(6));
      assertThat(redis.expire(k(6), 1)).isTrue();

      for (int i = 1; i <= 6; i++) {
         assertThat(redis.pttl(k(i))).isEqualTo(1_000L);
      }

      ((ControlledTimeService) timeService).advance(2_000);
      eventually(() -> redis.dbsize() == 0L);
   }

   @Test
   public void testExpireTime() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), v());
      assertThat(redis.expiretime(k())).isEqualTo(-1);
      assertThat(redis.expiretime(k(1))).isEqualTo(-2);
      redis.set(k(2), v(2), SetArgs.Builder.exAt(timeService.wallClockTime() + 10_000));
      assertThat(redis.expiretime(k(2))).isEqualTo(timeService.wallClockTime() + 10_000L);
   }

   @Test
   public void testPExpireTime() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), v());
      assertThat(redis.pexpiretime(k())).isEqualTo(-1);
      assertThat(redis.pexpiretime(k(1))).isEqualTo(-2);
      redis.set(k(2), v(2), SetArgs.Builder.exAt(timeService.wallClockTime() + 10_000));
      assertThat(redis.pexpiretime(k(2))).isEqualTo((timeService.wallClockTime() + 10_000L) * 1000L);
   }

   @Test
   public void testPersist() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), v(), SetArgs.Builder.ex(10_000));
      assertThat(redis.persist(k())).isTrue();
      redis.set(k(1), v(1));
      assertThat(redis.persist(k(1))).isFalse();
      assertThat(redis.persist(k(2))).isFalse();
   }

   @Test
   public void testMemoryUsage() {
      RedisCommands<String, String> redis = redisConnection.sync();
      // String test
      redis.set(k(), "1");
      // Memory usage for this entry is:
      // key: 16(header)+4(length)+4(padding) = 24
      // value: 16(header)+1(length)+7(padding) = 24
      // entry overhead: 24
      assertThat(redis.memoryUsage(k())).isEqualTo(72);
      redis.set(k(1), "a".repeat(1001));
      // Memory usage for this entry is:
      // key: 16(header)+18(length)+6(padding) = 40
      // value: 16(header)+1001(length)+7(padding) = 1024
      // entry overhead: 24
      assertThat(redis.memoryUsage(k(1))).isEqualTo(1088);

      // Check that memory usage doesn't fail for implemented data types
      // HashMap
      Map<String, String> map = Map.of("key1", "value1", "key2", "value2", "key3", "value3");
      redis.hmset(k(2), map);
      assertThat(redis.memoryUsage(k(2))).isPositive();
      // HyperLogLog
      redis.pfadd(k(3), "el1", "el2", "el3");
      assertThat(redis.memoryUsage(k(3))).isPositive();
      // List
      redis.rpush(k(4), "william", "jose", "pedro");
      assertThat(redis.memoryUsage(k(4))).isPositive();
      // Set
      redis.sadd(k(5), "1", "2", "3");
      assertThat(redis.memoryUsage(k(5))).isPositive();
      // SortedSet
      redis.zadd(k(6), 10.4, "william");
      assertThat(redis.memoryUsage(k(6))).isPositive();

      JsonPath jp = new JsonPath("$");
      DefaultJsonParser defaultJsonParser = new DefaultJsonParser();
      JsonValue jv = defaultJsonParser.createJsonValue("{\"key\":\"value\"}");
      redis.jsonSet(k(7), jp, jv);
      assertThat(redis.memoryUsage(k(7))).isPositive();
   }

   @Test
   public void testClusterNodesSingleNode() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThatThrownBy(redis::clusterNodes)
            .isInstanceOf(RedisCommandExecutionException.class)
            .hasMessageContaining("ERR This instance has cluster support disabled");
   }

   @Test
   public void testType() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), "1");
      assertThat(redis.type(k())).isEqualTo("string");
      redis.hset(k(1), "k", "v");
      assertThat(redis.type(k(1))).isEqualTo("hash");
      redis.lpush(k(2), "a");
      assertThat(redis.type(k(2))).isEqualTo("list");
      redis.sadd(k(3), "a");
      assertThat(redis.type(k(3))).isEqualTo("set");
      redis.zadd(k(4), 1.0, "a");
      assertThat(redis.type(k(4))).isEqualTo("zset");
      assertThat(redis.type(k(100))).isEqualTo("none");
   }

   @Test
   public void testExpire() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), v());
      assertThat(redis.ttl(k())).isEqualTo(-1);
      assertThat(redis.expire(k(), 1000)).isTrue();
      assertThat(redis.ttl(k())).isEqualTo(1000);
      assertThat(redis.expire(k(), 500, ExpireArgs.Builder.gt())).isFalse();
      assertThat(redis.expire(k(), 1500, ExpireArgs.Builder.gt())).isTrue();
      assertThat(redis.expire(k(), 2000, ExpireArgs.Builder.lt())).isFalse();
      assertThat(redis.expire(k(), 1000, ExpireArgs.Builder.lt())).isTrue();
      assertThat(redis.expire(k(), 1250, ExpireArgs.Builder.xx())).isTrue();
      assertThat(redis.expire(k(), 1000, ExpireArgs.Builder.nx())).isFalse();
      assertThat(redis.expire(k(1), 1000)).isFalse();
      redis.set(k(1), v(1));
      assertThat(redis.expire(k(1), 1000, ExpireArgs.Builder.xx())).isFalse();
      assertThat(redis.expire(k(1), 1000, ExpireArgs.Builder.nx())).isTrue();

      // Assert entry is removed with negative expire.
      assertThat(redis.expire(k(1), -10)).isTrue();
      assertThat(redis.get(k(1))).isNull();

      redis.set(k(2), v(2));
      assertThat(redis.expire(k(2), 1000, ExpireArgs.Builder.gt())).isFalse();
      assertThat(redis.expire(k(2), 1000, ExpireArgs.Builder.lt())).isTrue();
   }

   public void testPExpire() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), v());
      assertThat(redis.ttl(k())).isEqualTo(-1);
      assertThat(redis.pexpire(k(), 1000)).isTrue();
      assertThat(redis.pttl(k())).isEqualTo(1000);
      assertThat(redis.pexpire(k(), 500, ExpireArgs.Builder.gt())).isFalse();
      assertThat(redis.pexpire(k(), 1500, ExpireArgs.Builder.gt())).isTrue();
      assertThat(redis.pexpire(k(), 2000, ExpireArgs.Builder.lt())).isFalse();
      assertThat(redis.pexpire(k(), 1000, ExpireArgs.Builder.lt())).isTrue();
      assertThat(redis.pexpire(k(), 1250, ExpireArgs.Builder.xx())).isTrue();
      assertThat(redis.pexpire(k(), 1000, ExpireArgs.Builder.nx())).isFalse();
      assertThat(redis.pexpire(k(1), 1000)).isFalse();
      redis.set(k(1), v(1));
      assertThat(redis.pexpire(k(1), 1000, ExpireArgs.Builder.xx())).isFalse();
      assertThat(redis.pexpire(k(1), 1000, ExpireArgs.Builder.nx())).isTrue();

      // Assert entry is removed with negative expire.
      assertThat(redis.pexpire(k(1), -10)).isTrue();
      assertThat(redis.get(k(1))).isNull();
   }

   @Test
   public void testExpireAt() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), v());
      assertThat(redis.expiretime(k())).isEqualTo(-1);
      assertThat(redis.expireat(k(), timeService.wallClockTime() + 1000)).isTrue();
      assertThat(redis.expiretime(k())).isEqualTo(timeService.wallClockTime() + 1000);
      assertThat(redis.expireat(k(), timeService.wallClockTime() + 500, ExpireArgs.Builder.gt())).isFalse();
      assertThat(redis.expireat(k(), timeService.wallClockTime() + 1500, ExpireArgs.Builder.gt())).isTrue();
      assertThat(redis.expireat(k(), timeService.wallClockTime() + 2000, ExpireArgs.Builder.lt())).isFalse();
      assertThat(redis.expireat(k(), timeService.wallClockTime() + 1000, ExpireArgs.Builder.lt())).isTrue();
      assertThat(redis.expireat(k(), timeService.wallClockTime() + 1250, ExpireArgs.Builder.xx())).isTrue();
      assertThat(redis.expireat(k(), timeService.wallClockTime() + 1000, ExpireArgs.Builder.nx())).isFalse();
      assertThat(redis.expireat(k(1), timeService.wallClockTime() + 1000)).isFalse();
      redis.set(k(1), v(1));
      assertThat(redis.expireat(k(1), timeService.wallClockTime() + 1000, ExpireArgs.Builder.xx())).isFalse();
      assertThat(redis.expireat(k(1), timeService.wallClockTime() + 1000, ExpireArgs.Builder.nx())).isTrue();

      // Assert entry is removed when expiring in the past.
      assertThat(redis.expireat(k(1), TimeUnit.MILLISECONDS.toSeconds(timeService.wallClockTime() - 500))).isTrue();
      assertThat(redis.get(k(1))).isNull();
   }

   @Test
   public void testPExpireAt() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set(k(), v());
      assertThat(redis.expiretime(k())).isEqualTo(-1);
      assertThat(redis.pexpireat(k(), timeService.wallClockTime() + 1000)).isTrue();
      assertThat(redis.expiretime(k())).isEqualTo((timeService.wallClockTime() + 1000) / 1000);
      assertThat(redis.pexpireat(k(), timeService.wallClockTime() + 500, ExpireArgs.Builder.gt())).isFalse();
      assertThat(redis.pexpireat(k(), timeService.wallClockTime() + 1500, ExpireArgs.Builder.gt())).isTrue();
      assertThat(redis.pexpireat(k(), timeService.wallClockTime() + 2000, ExpireArgs.Builder.lt())).isFalse();
      assertThat(redis.pexpireat(k(), timeService.wallClockTime() + 1000, ExpireArgs.Builder.lt())).isTrue();
      assertThat(redis.pexpireat(k(), timeService.wallClockTime() + 1250, ExpireArgs.Builder.xx())).isTrue();
      assertThat(redis.pexpireat(k(), timeService.wallClockTime() + 1000, ExpireArgs.Builder.nx())).isFalse();
      assertThat(redis.pexpireat(k(1), timeService.wallClockTime() + 1000)).isFalse();
      redis.set(k(1), v(1));
      assertThat(redis.pexpireat(k(1), timeService.wallClockTime() + 1000, ExpireArgs.Builder.xx())).isFalse();
      assertThat(redis.pexpireat(k(1), timeService.wallClockTime() + 1000, ExpireArgs.Builder.nx())).isTrue();

      // Assert entry is removed when expiring in the past.
      assertThat(redis.pexpireat(k(1), timeService.wallClockTime() - 500)).isTrue();
      assertThat(redis.get(k(1))).isNull();
   }

   @Test
   public void testTouch() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.touch("unexisting")).isZero();

      redis.set("hello", "world");
      redis.rpush("list", "one", "two", "three");

      assertThat(redis.touch("hello", "list", "unexisting")).isEqualTo(2);
   }

   @Test
   public void testSort() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.sort("not_existing")).isEmpty();
      // sort ro is like sort but with store disabled. we only test here the command is available.
      assertThat(redis.sortReadOnly("not_existing")).isEmpty();

      // ******--
      // Lists
      // ******
      // RPUSH numbers 1 3 4 8 1 0 -1 19 -22 3
      redis.rpush("numbers", "1", "3", "4", "8", "1", "0", "-1", "19", "-22", "3");
      // LRANGE numbers 0 -1
      assertThat(redis.lrange("numbers", 0, -1))
            .containsExactly("1", "3", "4", "8", "1", "0", "-1", "19", "-22", "3");
      // SORT numbers
      assertThat(redis.sort("numbers"))
            .containsExactly("-22", "-1", "0", "1", "1", "3", "3", "4", "8", "19");
      // SORT numbers ALPHA
      assertThat(redis.sort("numbers", SortArgs.Builder.alpha()))
            .containsExactly("-1", "-22", "0", "1", "1", "19", "3", "3", "4", "8");
      // SORT numbers ALPHA DESC
      assertThat(redis.sort("numbers", SortArgs.Builder.alpha().desc()))
            .containsExactly("8", "4", "3", "3", "19", "1", "1", "0", "-22", "-1");
      // SORT numbers ALPHA DESC LIMIT 2 6
      assertThat(redis.sort("numbers", SortArgs.Builder.alpha().desc().limit(2, 6)))
            .containsExactly("3", "3", "19", "1", "1", "0");
      // SORT numbers ALPHA DESC LIMIT 2 0
      assertThat(redis.sort("numbers", SortArgs.Builder.alpha().desc().limit(2, 0)))
            .isEmpty();
      // SORT numbers ALPHA DESC STORE result_sset
      assertThat(redis.sortStore("numbers", SortArgs.Builder.alpha().desc(), "result_list"))
            .isEqualTo(10);
      // LRANGE result_list 0 -1
      assertThat(redis.lrange("result_list", 0, -1))
            .containsExactly("8", "4", "3", "3", "19", "1", "1", "0", "-22", "-1");
      // SORT numbers BY nosort
      assertThat(redis.sort("numbers", SortArgs.Builder.by("nosort")))
            .containsExactly("1", "3", "4", "8", "1", "0", "-1", "19", "-22", "3");
      // SORT numbers BY nosort LIMIT 0 1
      assertThat(redis.sort("numbers", SortArgs.Builder.by("nosort").limit(0, 1)))
            .containsExactly("1");
      // SORT numbers DESC BY nosort
      assertThat(redis.sort("numbers", SortArgs.Builder.by("nosort").desc()))
            .containsExactly("3", "-22", "19", "-1", "0", "1", "8", "4", "3", "1");
      // SORT numbers DESC BY nosort LIMIT 0 1
      assertThat(redis.sort("numbers", SortArgs.Builder.by("nosort").limit(0, 1).desc()))
            .containsExactly("3");
      // SORT numbers BY something
      assertThat(redis.sort("numbers", SortArgs.Builder.by("something")))
            .containsExactly("1", "3", "4", "8", "1", "0", "-1", "19", "-22", "3");
      // SORT numbers BY w_*
      assertThat(redis.sort("numbers", SortArgs.Builder.by("w_*")))
            .containsExactly("-1", "-22", "0", "1", "1", "19", "3", "3", "4", "8");
      // RPUSH people ryan tristan pedro
      redis.rpush("people", "ryan", "tristan", "pedro", "michael");
      // SORT people BY w_*
      assertThat(redis.sort("people", SortArgs.Builder.by("w_*")))
            .containsExactly("michael", "pedro", "ryan", "tristan");

      // Insert objects for hash retrieval in sort.
      redis.hset("h_ryan", Map.of("component", "persistence", "weight", "1"));
      redis.hset("h_pedro", Map.of("component", "cross-site", "weight", "2"));
      redis.hset("h_tristan", Map.of("component", "security", "weight", "3"));
      redis.hset("h_michael", Map.of("weight", "4"));

      // SET w_ryan 1
      redis.set("w_ryan", "1");
      // SET w_pedro 2
      redis.set("w_pedro", "2");
      // SET w_tristan 3
      redis.set("w_tristan", "3");
      // SET w_michael 4
      redis.set("w_michael", "4");

      // SORT people BY w_*
      assertThat(redis.sort("people", SortArgs.Builder.by("w_*")))
            .containsExactly("ryan", "pedro", "tristan", "michael");

      // SORT people BY h_*->weight
      assertThat(redis.sort("people", SortArgs.Builder.by("h_*->weight")))
            .containsExactly("ryan", "pedro", "tristan", "michael");

      // SET o_ryan 1
      redis.set("o_ryan", "persistence");
      // SET o_pedro 2
      redis.set("o_pedro", "cross-site");
      // SET o_tristan 3
      redis.set("o_tristan", "security");
      // SORT people BY w_* GET o_*
      assertThat(redis.sort("people", SortArgs.Builder.by("w_*").get("o_*")))
            .containsExactly("persistence", "cross-site", "security", null);

      // SORT people BY w_* GET h_*->component
      assertThat(redis.sort("people", SortArgs.Builder.by("w_*").get("h_*->component")))
            .containsExactly("persistence", "cross-site", "security", null);

      // SORT people BY w_* GET o_* GET #
      assertThat(redis.sort("people", SortArgs.Builder.by("w_*").get("o_*").get("#")))
            .containsExactly("persistence", "ryan", "cross-site", "pedro",
                  "security", "tristan", null, "michael");
      // SORT people BY w_* GET h_*->component GET #
      assertThat(redis.sort("people", SortArgs.Builder.by("w_*").get("h_*->component").get("#")))
            .containsExactly("persistence", "ryan", "cross-site", "pedro",
                  "security", "tristan", null, "michael");
      // SORT people BY w_* GET o GET #
      assertThat(redis.sort("people", SortArgs.Builder.by("w_*").get("o").get("#")))
            .containsExactly(null, "ryan", null, "pedro",
                  null, "tristan", null, "michael");

      // FIXME: Operation against hash keys is a wrong type, should return null.
      //  The operation succeeds if the storage type is Protostream since it can convert the bucket to byte[].
      // SORT people BY w_* GET h_* GET #
      //assertThat(redis.sort("people", SortArgs.Builder.by("w_*").get("h_*").get("#")))
      //      .containsExactly( null, "ryan", null, "pedro",
      //            null, "tristan", null, "michael");

      // *****
      // Sets
      // *****
      // SADD set_numbers 1 3 4 8 1 0 -1 19 -22 3
      redis.sadd("set_numbers", "1", "3", "4", "8", "1", "0", "-1", "19", "-22", "3");
      // SORT numbers
      assertThat(redis.sort("set_numbers"))
            .containsExactly("-22", "-1", "0", "1", "3", "4", "8", "19");
      // SORT set_numbers ALPHA
      assertThat(redis.sort("set_numbers", SortArgs.Builder.alpha()))
            .containsExactly("-1", "-22", "0", "1", "19", "3", "4", "8");
      // SORT set_numbers ALPHA DESC
      assertThat(redis.sort("set_numbers", SortArgs.Builder.alpha().desc()))
            .containsExactly("8", "4", "3", "19", "1", "0", "-22", "-1");
      // SORT set_numbers ALPHA DESC STORE result_sset
      assertThat(redis.sortStore("set_numbers", SortArgs.Builder.alpha().desc(), "result_sset"))
            .isEqualTo(8);
      // LRANGE result_zset 0 -1
      assertThat(redis.lrange("result_sset", 0, -1))
            .containsExactly("8", "4", "3", "19", "1", "0", "-22", "-1");

      // ************
      // Sorted Sets
      // ************
      // ZADD zset_numbers 1 9 2 8 3 15 4 -4 5 2 5 6 6 7 6 -3 6 1
      redis.zadd("zset_numbers", ZAddArgs.Builder.ch(),
            just(1, "9"),
            just(2, "8"),
            just(3, "15"),
            just(4, "-4"),
            just(5, "2"),
            just(5, "6"),
            just(6, "7"),
            just(6, "-3"),
            just(6, "1"));
      // SORT zset_numbers
      assertThat(redis.sort("zset_numbers"))
            .containsExactly("-4", "-3", "1", "2", "6", "7", "8", "9", "15");
      // SORT zset_numbers ALPHA
      assertThat(redis.sort("zset_numbers", SortArgs.Builder.alpha()))
            .containsExactly("-3", "-4", "1", "15", "2", "6", "7", "8", "9");
      // SORT zset_numbers ALPHA DESC
      assertThat(redis.sort("zset_numbers", SortArgs.Builder.alpha().desc()))
            .containsExactly("9", "8", "7", "6", "2", "15", "1", "-4", "-3");
      // SORT zset_numbers by no-sort
      assertThat(redis.sort("zset_numbers", SortArgs.Builder.by("no-sort")))
            .containsExactly("9", "8", "15", "-4", "2", "6", "-3", "1", "7");
      // SORT zset_numbers ALPHA DESC STORE result_zset
      assertThat(redis.sortStore("zset_numbers", SortArgs.Builder.alpha().desc(), "result_zset"))
            .isEqualTo(9);
      // LRANGE result_zset 0 -1
      assertThat(redis.lrange("result_zset", 0, -1))
            .containsExactly("9", "8", "7", "6", "2", "15", "1", "-4", "-3");

      assertWrongType(() -> redis.set("another", "tristan"), () -> redis.sort("another"));
   }

   @Test
   public void testRename() {
      RedisCommands<String, String> redis = redisConnection.sync();
      var srcKey = k(0);
      var dstKey = k(1);
      var val = v();
      redis.set(srcKey, val);
      redis.rename(srcKey, dstKey);
      assertThat(redis.get(dstKey)).isEqualTo(val);
      Exceptions.expectException(RedisCommandExecutionException.class,
            "ERR no such key",
            () -> redis.rename("not-existent", dstKey));
   }

   @Test
   public void testRenameList() {
      RedisCommands<String, String> redis = redisConnection.sync();
      var srcKey = k(0);
      var dstKey = k(1);
      var val = v();
      redis.rpush(srcKey, val);
      redis.rename(srcKey, dstKey);
      assertThat(redis.lrange(dstKey, 0, -1)).containsExactly(val);
   }


   @Test
   public void testRenameWithEx() {
      RedisCommands<String, String> redis = redisConnection.sync();
      var srcKey = k(0);
      var dstKey = k(1);
      var val = v();
      var setArg = new SetArgs();
      setArg.ex(60);
      redis.set(srcKey, val, setArg);
      ((ControlledTimeService) timeService).advance(30, TimeUnit.SECONDS);
      redis.rename(srcKey, dstKey);
      assertThat(redis.get(dstKey)).isEqualTo(val);
      var nowTs = timeService.wallClockTime();
      assertThat(redis.expiretime(dstKey) - nowTs / 1000).isLessThanOrEqualTo(30);
      ((ControlledTimeService) timeService).advance(35, TimeUnit.SECONDS);
      assertThat(redis.get(dstKey)).isNull();
   }

   @Test
   public void testRenamenx() {
      RedisCommands<String, String> redis = redisConnection.sync();
      var srcKey = k(0);
      var dstKey = k(1);
      var val = v();
      var val1 = v(1);
      redis.set(srcKey, val);
      assertThat(redis.renamenx(srcKey, dstKey)).isEqualTo(true);
      assertThat(redis.get(dstKey)).isEqualTo(val);
      redis.set(srcKey, val1);
      assertThat(redis.renamenx(srcKey, dstKey)).isEqualTo(false);
      assertThat(redis.get(dstKey)).isEqualTo(val);
      Exceptions.expectException(RedisCommandExecutionException.class,
            "ERR no such key",
            () -> redis.renamenx("not-existent", "not-existent-1"));
   }

   @Test
   public void testRenameTypes() {
      RedisCommands<String, String> redis = redisConnection.sync();
      var srcKey = k(0);
      var dstKey = k(1);
      var val = v();
      redis.rpush(srcKey, val);
      redis.rename(srcKey, dstKey);
      assertThat(redis.lrange(dstKey, 0, -1)).containsExactly(val);
   }

   @Test
   public void testTime() {
      RedisCommands<String, String> redis = redisConnection.sync();
      var redisNow = redis.time();
      var now = timeService.instant();
      assertThat(Integer.parseInt(redisNow.get(0))).isEqualTo(now.getEpochSecond());
      assertThat(Integer.parseInt(redisNow.get(1))).isEqualTo(TimeUnit.NANOSECONDS.toMicros(now.getNano()));
   }

   @Test
   public void testPFADD() {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Just creates the new structure. But Lettuce does not accept an empty argument.
      // assertThat(redis.pfadd("my-hll")).isEqualTo(1L);

      assertThat(redis.pfadd("my-hll", "el1", "el2", "el3"))
            .isEqualTo(1L);

      assertThat(redis.pfadd("my-hll", "el1", "el2", "el3")).isEqualTo(0L);

      // Until 193 it is using the explicit representation. It could only return false in case of a hash conflict.
      for (int i = 4; i < 193; i++) {
         assertThat(redis.pfadd("my-hll", "el" + i)).isEqualTo(1L);
      }

      // From this point on, it is using the probabilistic estimation.
      SoftAssertions sa = new SoftAssertions();
      for (int i = 0; i < 831; i++) {
         sa.assertThat(redis.pfadd("my-hll", "hello-" + i)).isEqualTo(1L);
      }

      assertThat(redis.pfadd("my-hll", "hello-0", "hello-1", "hello-2")).isEqualTo(0L);
      assertThat(sa.errorsCollected()).hasSize(16);
      // TODO: Verify cardinality ISPN-14676

      assertWrongType(() -> redis.set("plain", "string"), () -> redis.pfadd("plain", "el1"));
      // assertWrongType(() -> redis.pfadd("data", "e1"), () -> redis.get("data"));
   }

   @Test
   public void testKeys() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.flushdb();
      assertThat(redis.keys("*")).isEmpty();

      Set<String> all = new HashSet<>();
      for (int i = 0; i < 15; i++) {
         String k = "hello_" + i;
         redis.set(k, "world_" + i);
         all.add(k);
      }

      List<String> allKeysPatternResult = redis.keys("*");
      assertThat(allKeysPatternResult).hasSize(all.size());
      assertThat(allKeysPatternResult).containsExactlyInAnyOrderElementsOf(all);
      assertThat(redis.keys("*1")).containsExactlyInAnyOrder("hello_1", "hello_11");
      assertThat(redis.keys("hello_[2-4]")).containsExactlyInAnyOrder("hello_2", "hello_3", "hello_4");
      assertThat(redis.keys("hello_[24]")).containsExactlyInAnyOrder("hello_2", "hello_4");
      assertThat(redis.keys("hello_[^1]")).containsExactlyInAnyOrder("hello_0", "hello_2", "hello_3",
            "hello_4", "hello_5", "hello_6", "hello_7", "hello_8", "hello_9");
   }

   @Test
   public void testRandomKey() {
      RedisCommands<String, String> redis = redisConnection.sync();

      assertThat(redis.randomkey()).isNull();
      assertThat(redis.set("k1", "v1")).isEqualTo(OK);
      assertThat(redis.randomkey()).isEqualTo("k1");

      assertThat(redis.set("k2", "v2")).isEqualTo(OK);

      boolean c = false;
      for (int i = 0; i < 20; i++) {
         String k = redis.randomkey();
         if ("k2".equals(k)) {
            c = true;
            break;
         }
      }

      assertThat(c).isTrue();
   }

   @Test
   public void testDB() {
      ConfigurationBuilder builder = defaultRespConfiguration();
      amendConfiguration(builder);

      if (isAuthorizationEnabled()) {
         Security.doAs(ADMIN, () -> {
            manager(0).createCache("1", builder.build());
            manager(0).createCache("2", builder.build());
         });
      } else {
         manager(0).createCache("1", builder.build());
         manager(0).createCache("2", builder.build());
      }

      testDBInternal();
   }

   private void testDBInternal() {
      // Use a new connection to avoid conflicts with other tests
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.select(1)).isEqualTo("OK");
      redis.set(k(), v());
      assertThat(redis.get(k())).isEqualTo(v());
      assertThat(redis.select(2)).isEqualTo("OK");
      assertThat(redis.get(k())).isNull();
      redis.set(k(), v(1));
      assertThat(redis.get(k())).isEqualTo(v(1));
      assertThat(redis.select(1)).isEqualTo("OK");
      assertThat(redis.get(k())).isEqualTo(v());
      redis.select(0);
   }

   @Test
   public void testNoAuthHello() {
      SkipTestNG.skipIf(!isAuthorizationEnabled(), "Run only with authz enabled");

      // Tries to only establish the connection without AUTH parameters.
      RedisURI uri = RedisURI.Builder.redis(HOST, server.getPort()).build();
      try (RedisClient noAuthClient = RedisClient.create(uri)) {
         assertThatThrownBy(noAuthClient::connect)
               .isInstanceOf(RedisConnectionException.class)
               .cause()
               .isInstanceOf(RedisCommandExecutionException.class)
               .hasMessage("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time");
      }
   }

   @Test
   public void testLolwut() {
      RedisCommands<String, String> redis = redisConnection.sync();
      CommandArgs<String, String> args = new CommandArgs<>(StringCodec.UTF8);
      String response = redis.dispatch(new SimpleCommand("LOLWUT"), new StatusOutput<>(StringCodec.UTF8), args);
      assertThat(response).endsWith(Version.getBrandName() + " ver. " + Version.getBrandVersion() + "\n");
   }
}
