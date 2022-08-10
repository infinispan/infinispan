package org.infinispan.server.resp;

import static org.infinispan.server.resp.test.RespTestingUtil.createClient;
import static org.infinispan.server.resp.test.RespTestingUtil.killClient;
import static org.infinispan.server.resp.test.RespTestingUtil.killServer;
import static org.infinispan.server.resp.test.RespTestingUtil.startServer;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.test.Exceptions;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
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
   protected static final int timeout = 60;

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      cacheManager = createTestCacheManager();
      server = startServer(cacheManager);
      client = createClient(30000, server.getPort());
      redisConnection = client.connect();
      cache = cacheManager.getCache(server.getConfiguration().defaultCacheName());
      return cacheManager;
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
            handOffQueue.add("message-" + channel + "-" + message);
         }

         @Override
         public void subscribed(String channel, long count) {
            handOffQueue.add("subscribed-" + channel + "-" + count);
         }

         @Override
         public void unsubscribed(String channel, long count) {
            handOffQueue.add("unsubscribed-" + channel + "-" + count);
         }
      });

      return handOffQueue;
   }

   @DataProvider(name = "booleans")
   Object[][] booleans() {
      // Reset disabled for now as the client isn't sending a reset command to the server
      return new Object[][]{/*{true},*/ {false}};
   }

   @Test(dataProvider = "booleans")
   public void testPubSubUnsubscribe(boolean reset) throws InterruptedException {
      RedisPubSubCommands<String, String> connection = client.connectPubSub().sync();
      BlockingQueue<String> handOffQueue = addPubSubListener(connection);

      // Subscribe to some channels
      connection.subscribe("channel2", "test");
      String value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertEquals("subscribed-channel2-0", value);
      value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertEquals("subscribed-test-0", value);

      // Unsubscribe to all channels
      if (reset) {
         connection.reset();
      } else {
         connection.unsubscribe();
      }

      // Unsubscribed channels can be in different orders
      for (int i = 0; i < 2; ++i) {
         value = handOffQueue.poll(10, TimeUnit.SECONDS);
         assertNotNull("Didn't receive any notifications", value);
         if (!value.equals("unsubscribed-channel2-0") && !value.equals("unsubscribed-test-0")) {
            fail("Notification doesn't match expected, was: " + value);
         }
      }
   }

   public void testPubSub() throws InterruptedException {
      RedisPubSubCommands<String, String> connection = client.connectPubSub().sync();
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
      assertEquals(20, commands.size());
   }

   public void testAuth() {
      RedisCommands<String, String> redis = redisConnection.sync();
      Exceptions.expectException(RedisCommandExecutionException.class, ".*but no password is set", () -> redis.auth("user", "pass"));
   }
}
