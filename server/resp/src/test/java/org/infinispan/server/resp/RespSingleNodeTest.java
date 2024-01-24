package org.infinispan.server.resp;

import static org.infinispan.server.resp.test.RespTestingUtil.createClient;
import static org.infinispan.server.resp.test.RespTestingUtil.killClient;
import static org.infinispan.server.resp.test.RespTestingUtil.killServer;
import static org.infinispan.server.resp.test.RespTestingUtil.startServer;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.server.resp.test.CommonRespTests;
import org.infinispan.server.resp.test.RespTestingUtil;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;

import io.lettuce.core.SetArgs;
import org.testng.annotations.AfterClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;

/**
 * Base class for single node tests.
 *
 * @author William Burns
 * @since 14.0
 */
@Test(groups = "functional", testName = "server.resp.RespSingleNodeTest")
public class RespSingleNodeTest extends SingleCacheManagerTest {
   protected RedisClient client;
   protected RespServer server;
   protected StatefulRedisConnection<String, String> redisConnection;
   protected static final int timeout = 30_000;

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      cacheManager = createTestCacheManager();
      RespServerConfiguration serverConfiguration = serverConfiguration().build();
      server = startServer(cacheManager, serverConfiguration);
      client = createRedisClient(server.getPort());
      redisConnection = client.connect();
      cache = cacheManager.getCache(server.getConfiguration().defaultCacheName());
      return cacheManager;
   }

   protected RespServerConfigurationBuilder serverConfiguration() {
      String serverName = TestResourceTracker.getCurrentTestShortName();
      return new RespServerConfigurationBuilder().name(serverName)
            .host(RespTestingUtil.HOST)
            .port(RespTestingUtil.port());
   }

   protected RedisClient createRedisClient(int port) {
      return createClient(timeout, port);
   }

   protected EmbeddedCacheManager createTestCacheManager() {
      return TestCacheManagerFactory.createCacheManager(true);
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroyAfterClass() {
      super.destroyAfterClass();
      log.debug("Test finished, close resp server");
      killClient(client);
      killServer(server);
   }

   public void testSetMultipleOptions() throws Exception {
      RedisCommands<String, String> redis = redisConnection.sync();

      // Should return (nil), failed since value does not exist
      SetArgs args = SetArgs.Builder.xx();
      assertNull(redis.set("key", "value", args));

      // Should return OK, because value does not exist.
      args = SetArgs.Builder.nx();
      assertEquals("OK", redis.set("key", "value", args));
      assertEquals("value", redis.get("key"));

      // Should return (nil), because value exists.
      assertNull(redis.set("key", "value3", args));
      assertEquals("value", redis.get("key"));

      // Should return OK, value exists
      args = SetArgs.Builder.xx();
      assertEquals("OK", redis.set("key", "value2", args));
      assertEquals("value2", redis.get("key"));

      // Should insert with TTL. This one returns the value.
      args = SetArgs.Builder.ex(60);
      assertEquals("value2", redis.setGet("key", "value3", args));

      CacheEntry<Object, Object> entry = cache.getAdvancedCache()
            .withMediaType(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OCTET_STREAM)
            .getCacheEntry("key".getBytes(StandardCharsets.UTF_8));
      assertEquals("value3", new String((byte[]) entry.getValue()));
      assertEquals(60_000, entry.getLifespan());

      // Making sure we won't go that fast.
      Thread.sleep(50);

      // We insert while keeping the TTL.
      args = SetArgs.Builder.keepttl();
      assertEquals("OK", redis.set("key", "value4", args));
      entry = cache.getAdvancedCache()
            .withMediaType(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OCTET_STREAM)
            .getCacheEntry("key".getBytes(StandardCharsets.UTF_8));
      assertEquals("value4", new String((byte[]) entry.getValue()));
      assertTrue(entry.getLifespan() < 60_000);

      // Conditional operation keeping TTL.
      args = SetArgs.Builder.keepttl().xx();
      assertEquals("OK", redis.set("key", "value5", args));
      entry = cache.getAdvancedCache()
            .withMediaType(MediaType.APPLICATION_OCTET_STREAM, MediaType.APPLICATION_OCTET_STREAM)
            .getCacheEntry("key".getBytes(StandardCharsets.UTF_8));
      assertEquals("value5", new String((byte[]) entry.getValue()));
      assertTrue(entry.getLifespan() < 60_000);

      // Key exist and keeping TTL, but conditional failing. Should return nil.
      args = SetArgs.Builder.keepttl().nx();
      String res = redis.set("key", "value5", args);
      assertNull("Should be null: " + res, res);

      // No NPE when keeping TTL, key not exists, and conditional succeed.
      args = SetArgs.Builder.keepttl().nx();
      assertEquals("OK", redis.set("randomKey", "value", args));

      // No NPE when keeping TTL and key doesn't exist.
      args = SetArgs.Builder.keepttl();
      assertEquals("OK", redis.set("otherKey", "value", args));

      redis.del("key", "randomKey", "otherKey");
   }

   public void testConditionalSetOperationWithReturn() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = UUID.randomUUID().toString();

      // Should return (nil), failed since value does not exist.
      SetArgs args = SetArgs.Builder.xx();
      assertNull(redis.setGet(key, "something", args));
      assertNull(redis.get(key));

      // Should return (nil), because value does not exist, but operation succeeded.
      args = SetArgs.Builder.nx();
      String res = redis.setGet(key, "value", args);
      assertNull("Should be null: " + res, res);
      assertEquals("value", redis.get(key));

      // Should return the previous because value exists but operation failed.
      assertEquals("value", redis.setGet(key, "value2", args));
      assertEquals("value", redis.get(key));

      // Should return previous value but succeeded.
      args = SetArgs.Builder.xx();
      assertEquals("value", redis.setGet(key, "value2", args));
      assertEquals("value2", redis.get(key));
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
      assertEquals(expected, results);
   }

   public void testSetEmptyStringMGet() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set("k1", "");
      redis.set("k3", "value2");

      assertEquals("", redis.get("k1"));

      List<KeyValue<String, String>> expected = new ArrayList<>(3);
      expected.add(KeyValue.just("k1", ""));
      expected.add(KeyValue.empty("k2"));
      expected.add(KeyValue.just("k3", "value2"));

      List<KeyValue<String, String>> results = redis.mget("k1", "k2", "k3");
      assertEquals(expected, results);
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
      assertEquals(expected, results);
   }

   public void testSetGetDelete() {
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.set("k1", "v1");
      String v = redis.get("k1");
      assertEquals("v1", v);

      redis.del("k1");

      assertNull(redis.get("k1"));
      assertNull(redis.get("something"));
   }

   public void testSetGetBigValue() {
      RedisCommands<String, String> redis = redisConnection.sync();
      StringBuilder sb = new StringBuilder();
      String charsToChoose = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";

      for(int i = 0; i < 10_000; ++i) {
         sb.append(charsToChoose.charAt(ThreadLocalRandom.current().nextInt(charsToChoose.length())));
      }
      String actualString = sb.toString();
      redis.set("k1", actualString);
      String v = redis.get("k1");
      assertEquals(actualString, v);
   }

   public void testPingNoArg() {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertEquals("PONG", redis.ping());
   }

   public void testEcho() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String stringToSend = "HI THERE!";
      assertEquals(stringToSend, redis.echo(stringToSend));
   }

   private BlockingQueue<String> addPubSubListener(RedisPubSubCommands<String, String> connection) {
      BlockingQueue<String> handOffQueue = new LinkedBlockingQueue<>();

      connection.getStatefulConnection().addListener(new RedisPubSubAdapter<String, String>() {
         @Override
         public void message(String channel, String message) {
            log.tracef("Received message on channel %s of %s", channel, message);
            handOffQueue.add("message-" + channel + "-" + message);
         }

         @Override
         public void subscribed(String channel, long count) {
            log.tracef("Subscribed to %s with %s", channel, count);
            handOffQueue.add("subscribed-" + channel + "-" + count);
         }

         @Override
         public void unsubscribed(String channel, long count) {
            log.tracef("Unsubscribed to %s with %s", channel, count);
            handOffQueue.add("unsubscribed-" + channel + "-" + count);
         }
      });

      return handOffQueue;
   }

   @DataProvider(name = "booleans")
   Object[][] booleans() {
      // Reset disabled for now as the client isn't sending a reset command to the server
      return new Object[][]{{true}, {false}};
   }

   @Test(dataProvider = "booleans")
   public void testPubSubUnsubscribe(boolean quit) throws InterruptedException {
      int listenersBefore = cache.getAdvancedCache().getListeners().size();

      RedisPubSubCommands<String, String> connection = createPubSubConnection();
      BlockingQueue<String> handOffQueue = addPubSubListener(connection);

      // Subscribe to some channels
      connection.subscribe("channel2", "test");
      String value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertEquals("subscribed-channel2-0", value);
      value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertEquals("subscribed-test-0", value);


      // 2 listeners, one for each sub above
      assertEquals(listenersBefore + 2, cache.getAdvancedCache().getListeners().size());
      // Unsubscribe to all channels
      if (quit) {
         // Originally wanted to use reset or quit, but they don't do what we expect from lettuce
         connection.getStatefulConnection().close();

         // Have to use eventually as they are removed asynchronously
         eventuallyEquals(listenersBefore, () -> cache.getAdvancedCache().getListeners().size());

         assertTrue(handOffQueue.isEmpty());
      } else {
         connection.unsubscribe();

         // Unsubscribed channels can be in different orders
         for (int i = 0; i < 2; ++i) {
            value = handOffQueue.poll(10, TimeUnit.SECONDS);
            assertNotNull("Didn't receive any notifications", value);
            if (!value.equals("unsubscribed-channel2-0") && !value.equals("unsubscribed-test-0")) {
               fail("Notification doesn't match expected, was: " + value);
            }
         }

         assertEquals(listenersBefore, cache.getAdvancedCache().getListeners().size());
         assertEquals("PONG", connection.ping());
      }
   }

   public void testPubSub() throws InterruptedException {
      RedisPubSubCommands<String, String> connection = createPubSubConnection();
      BlockingQueue<String> handOffQueue = addPubSubListener(connection);
      // Subscribe to some channels
      connection.subscribe("channel2", "test");
      String value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertEquals("subscribed-channel2-0", value);
      value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertEquals("subscribed-test-0", value);

      // Send a message to confirm it is properly listening
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.publish("channel2", "boomshakayaka");
      value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertEquals("message-channel2-boomshakayaka", value);

      connection.subscribe("channel");
      value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertEquals("subscribed-channel-0", value);

      connection.unsubscribe("channel2");
      connection.unsubscribe("doesn't-exist");
      connection.unsubscribe("channel", "test");

      for (String channel : new String[] {"channel2", "doesn't-exist", "channel", "test"}) {
         value = handOffQueue.poll(10, TimeUnit.SECONDS);
         assertEquals("unsubscribed-" + channel + "-0", value);
      }
   }

   public void testIncrNotPresent() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String nonPresentKey = "incr-notpresent";
      Long newValue = redis.incr(nonPresentKey);
      assertEquals(1L, newValue.longValue());

      Long nextValue = redis.incr(nonPresentKey);
      assertEquals(2L, nextValue.longValue());
   }

   public void testIncrPresent() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "incr";
      redis.set(key, "12");

      Long newValue = redis.incr(key);
      assertEquals(13L, newValue.longValue());

      Long nextValue = redis.incr(key);
      assertEquals(14L, nextValue.longValue());
   }

   public void testIncrPresentNotInteger() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "incr-string";
      redis.set(key, "foo");

      Exceptions.expectException(RedisCommandExecutionException.class, ".*value is not an integer or out of range", () -> redis.incr(key));
   }

   public void testDecrNotPresent() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String nonPresentKey = "decr-notpresent";
      Long newValue = redis.decr(nonPresentKey);
      assertEquals(-1L, newValue.longValue());

      Long nextValue = redis.decr(nonPresentKey);
      assertEquals(-2L, nextValue.longValue());
   }

   public void testDecrPresent() {
      RedisCommands<String, String> redis = redisConnection.sync();
      String key = "decr";
      redis.set(key, "12");

      Long newValue = redis.decr(key);
      assertEquals(11L, newValue.longValue());

      Long nextValue = redis.decr(key);
      assertEquals(10L, nextValue.longValue());
   }

   public void testCommand() {
      RedisCommands<String, String> redis = redisConnection.sync();

      List<Object> commands = redis.command();
      assertEquals(21, commands.size());
   }

   public void testAuth() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Exceptions.expectException(RedisCommandExecutionException.class, "WRONGPASS invalid username-password pair or user is disabled\\.", () -> redis.auth("user", "pass"));
   }

   public void testNotImplementedCommand() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Exceptions.expectException(RedisCommandExecutionException.class, "ERR unknown command", () -> redis.sadd("not-supported", "should error"));
   }

   protected RedisPubSubCommands<String, String> createPubSubConnection() {
      return client.connectPubSub().sync();
   }

   public void testPipeline() throws ExecutionException, InterruptedException, TimeoutException {
      CommonRespTests.testPipeline(redisConnection);
   }
}
