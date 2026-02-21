package org.infinispan.server.resp.pubsub;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.infinispan.server.resp.test.RespTestingUtil.PONG;
import static org.infinispan.test.TestingUtil.getListeners;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.infinispan.server.resp.SingleNodeRespBaseTest;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;

@Test(groups = "functional", testName = "dist.server.resp.PublishSubscribeTest")
public class PublishSubscribeTest extends SingleNodeRespBaseTest {

   @Override
   public Object[] factory() {
      return new Object[]{
         new PublishSubscribeTest(),
         new PublishSubscribeTest().withAuthorization()
      };
   }

   @DataProvider(name = "booleans")
   protected Object[][] booleans() {
      // Reset disabled for now as the client isn't sending a reset command to the
      // server
      return new Object[][] { { true }, { false } };
   }

   public void testPubSubChannels() throws Exception {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.pubsubChannels()).isEmpty();

      RedisPubSubCommands<String, String> connection = createPubSubConnection();
      BlockingQueue<String> handOffQueue = addPubSubListener(connection);

      connection.subscribe("channel-1", "channel-2");
      assertSubscription(handOffQueue, "channel-1", "channel-2");

      assertThat(redis.pubsubChannels())
            .hasSize(2)
            .containsExactlyInAnyOrder("channel-1", "channel-2");

      connection.unsubscribe("channel-1");
      assertThat(redis.pubsubChannels())
            .hasSize(1)
            .containsExactly("channel-2");

      connection.unsubscribe("channel-2");
      assertThat(redis.pubsubChannels()).isEmpty();
   }

   public void testPubSubChannelsFiltering() throws Exception {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.pubsubChannels()).isEmpty();

      RedisPubSubCommands<String, String> connection = createPubSubConnection();
      BlockingQueue<String> handOffQueue = addPubSubListener(connection);

      connection.subscribe("tx-channel", "kv-channel-1", "kv-channel-2");
      assertSubscription(handOffQueue, "tx-channel", "kv-channel-1", "kv-channel-2");

      assertThat(redis.pubsubChannels("*"))
            .hasSize(3)
            .containsExactlyInAnyOrder("tx-channel", "kv-channel-1", "kv-channel-2");

      assertThat(redis.pubsubChannels("tx-*"))
            .hasSize(1)
            .containsExactlyInAnyOrder("tx-channel");

      assertThat(redis.pubsubChannels("kv-*"))
            .hasSize(2)
            .containsExactlyInAnyOrder("kv-channel-1", "kv-channel-2");

      assertThat(redis.pubsubChannels("*-channel*"))
            .hasSize(3)
            .containsExactlyInAnyOrder("tx-channel", "kv-channel-1", "kv-channel-2");

      connection.unsubscribe("tx-channel", "kv-channel-1", "kv-channel-2");
   }

   public void testPubSubMultipleClientsSameChannel() throws Exception {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.pubsubChannels()).isEmpty();

      RedisPubSubCommands<String, String> client1 = createPubSubConnection();
      BlockingQueue<String> queue1 = addPubSubListener(client1);

      RedisPubSubCommands<String, String> client2 = createPubSubConnection();
      BlockingQueue<String> queue2 = addPubSubListener(client2);

      // Different connections subscribe to the same channel.
      // This should count as one single active channel.
      client1.subscribe("default-channel");
      client2.subscribe("default-channel");

      assertSubscription(queue1, "default-channel");
      assertSubscription(queue2, "default-channel");

      // Both clients are subscribed but to the same channel.
      assertThat(redis.pubsubChannels())
            .hasSize(1)
            .containsExactly("default-channel");

      client1.unsubscribe("default-channel");
      assertUnsubscribe(queue1, "default-channel");

      // One of the clients still subscribed.
      assertThat(redis.pubsubChannels())
            .hasSize(1)
            .containsExactly("default-channel");

      // Remove the last subscriber.
      client2.unsubscribe("default-channel");
      assertUnsubscribe(queue2, "default-channel");

      // Nothing remaining.
      assertThat(redis.pubsubChannels()).isEmpty();
   }

   public void testDifferentConnectionsCounting() throws Exception {
      RedisCommands<String, String> redis1 = redisConnection.sync();
      RedisCommands<String, String> redis2 = redisConnection.sync();
      assertThat(redis1.pubsubChannels()).isEmpty();
      assertThat(redis2.pubsubChannels()).isEmpty();

      RedisPubSubCommands<String, String> connection = createPubSubConnection();
      BlockingQueue<String> queue = addPubSubListener(connection);

      connection.subscribe("global-channel");
      assertSubscription(queue, "global-channel");

      assertThat(redis1.pubsubChannels())
            .hasSize(1)
            .containsExactly("global-channel");

      assertThat(redis2.pubsubChannels())
            .hasSize(1)
            .containsExactly("global-channel");

      connection.unsubscribe("global-channel");
      assertUnsubscribe(queue, "global-channel");

      assertThat(redis1.pubsubChannels()).isEmpty();
      assertThat(redis2.pubsubChannels()).isEmpty();
   }

   @Test(dataProvider = "booleans")
   public void testPubSubUnsubscribe(boolean quit) throws InterruptedException {
      int listenersBefore = getListeners(cache).size();

      RedisPubSubCommands<String, String> connection = createPubSubConnection();
      BlockingQueue<String> handOffQueue = addPubSubListener(connection);

      // Subscribe to some channels
      connection.subscribe("channel2", "test");
      assertSubscription(handOffQueue, "channel2", "test");

      // 2 listeners, one for each sub above
      assertThat(getListeners(cache)).hasSize(listenersBefore + 2);
      // Unsubscribe to all channels
      if (quit) {
         // Originally wanted to use reset or quit, but they don't do what we expect from
         // lettuce
         connection.getStatefulConnection().close();

         // Have to use eventually as they are removed asynchronously
         eventually(() -> getListeners(cache).size() == listenersBefore);
         assertThat(getListeners(cache)).hasSize(listenersBefore);

         assertThat(handOffQueue).isEmpty();
      } else {
         connection.unsubscribe();

         // Unsubscribed channels can be in different orders
         for (int i = 0; i < 2; ++i) {
            String value = handOffQueue.poll(10, TimeUnit.SECONDS);
            assertThat(value).withFailMessage("Didn't receive any notifications").isNotNull();
            if (!value.startsWith("unsubscribed-channel2-") && !value.startsWith("unsubscribed-test-")
                  && (!value.endsWith("0") || !value.endsWith("1"))) {
               fail("Notification doesn't match expected, was: " + value);
            }
         }

         assertThat(getListeners(cache)).hasSize(listenersBefore);
         assertThat(connection.ping()).isEqualTo(PONG);
      }
   }

   @Test
   public void testPubSub() throws InterruptedException {
      RedisPubSubCommands<String, String> connection = createPubSubConnection();
      BlockingQueue<String> handOffQueue = addPubSubListener(connection);
      // Subscribe to some channels
      connection.subscribe("channel2", "test");
      assertSubscription(handOffQueue, "channel2", "test");

      // Send a message to confirm it is properly listening
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.publish("channel2", "boomshakayaka");
      String value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertThat(value).isEqualTo("message-channel2-boomshakayaka");

      connection.subscribe("channel");
      value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertThat(value).isEqualTo("subscribed-channel-3");

      connection.unsubscribe("channel2");
      connection.unsubscribe("doesn't-exist");
      connection.unsubscribe("channel", "test");

      int subscriptions = 3;
      for (String channel : new String[] { "channel2", "doesn't-exist", "channel", "test" }) {
         value = handOffQueue.poll(10, TimeUnit.SECONDS);
         assertThat(value).isEqualTo("unsubscribed-" + channel + "-" + Math.max(0, --subscriptions));
      }
   }

   @Test
   public void testCountOnlyPatterns() throws Exception {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.pubsubNumpat()).isZero();

      RedisPubSubCommands<String, String> connection = createPubSubConnection();
      BlockingQueue<String> handOffQueue = addPubSubListener(connection);

      // Subscribe to some channels
      connection.subscribe("channel2", "test");
      assertSubscription(handOffQueue, "channel2", "test");

      // We have two subscribers
      assertThat(redis.pubsubChannels()).containsExactlyInAnyOrder("channel2", "test");

      // But they are not patterns.
      assertThat(redis.pubsubNumpat()).isZero();

      connection.unsubscribe("channel2", "test");
   }

   @Test
   public void testSubscribeToOneChannelMoreThanOnce() throws InterruptedException {
      RedisPubSubCommands<String, String> connection = createPubSubConnection();
      BlockingQueue<String> handOffQueue = addPubSubListener(connection);

      // Subscribe to the same channel multiple times
      connection.subscribe("chan1");
      assertSubscription(handOffQueue, "chan1");

      connection.subscribe("chan1");
      String value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertThat(value).isNotNull();
      assertThat(value).startsWith("subscribed-chan1-");

      connection.subscribe("chan1");
      value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertThat(value).isNotNull();
      assertThat(value).startsWith("subscribed-chan1-");

      // Publish should only deliver one copy (no duplicate listeners)
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.publish("chan1", "hello");
      value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertThat(value).isEqualTo("message-chan1-hello");

      // No duplicate message
      value = handOffQueue.poll(1, TimeUnit.SECONDS);
      assertThat(value).isNull();

      connection.unsubscribe("chan1");
   }

   @Test
   public void testUnsubscribeFromNonSubscribedChannels() throws InterruptedException {
      RedisPubSubCommands<String, String> connection = createPubSubConnection();
      BlockingQueue<String> handOffQueue = addPubSubListener(connection);

      // Enter subscriber mode first
      connection.subscribe("dummy");
      assertSubscription(handOffQueue, "dummy");

      // Unsubscribe from channels we never subscribed to
      connection.unsubscribe("foo", "bar", "quux");

      // Should get unsubscribe confirmations - count stays at 1 (still subscribed to "dummy")
      for (String channel : new String[] { "foo", "bar", "quux" }) {
         String value = handOffQueue.poll(10, TimeUnit.SECONDS);
         assertThat(value).withFailMessage("Didn't receive unsubscribe notification for " + channel).isNotNull();
         assertThat(value).startsWith("unsubscribed-" + channel + "-");
      }

      connection.unsubscribe("dummy");
   }

   // --- Pattern subscription tests (PSUBSCRIBE / PUNSUBSCRIBE) ---

   @Test
   public void testPSubscribeBasics() throws InterruptedException {
      RedisPubSubCommands<String, String> connection = createPubSubConnection();
      BlockingQueue<String> handOffQueue = addPubSubListener(connection);

      // Subscribe to two patterns
      connection.psubscribe("foo.*", "bar.*");
      assertPSubscription(handOffQueue, "foo.*", "bar.*");

      RedisCommands<String, String> redis = redisConnection.sync();

      // Publish to channels matching the patterns
      redis.publish("foo.1", "hello");
      String value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertThat(value).isEqualTo("pmessage-foo.*-foo.1-hello");

      redis.publish("bar.1", "world");
      value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertThat(value).isEqualTo("pmessage-bar.*-bar.1-world");

      // Publish to channels that should NOT match
      redis.publish("foo1", "nope");
      redis.publish("barfoo.1", "nope");
      redis.publish("qux.1", "nope");

      // Give a brief moment for any unexpected messages
      value = handOffQueue.poll(1, TimeUnit.SECONDS);
      assertThat(value).isNull();

      // Unsubscribe from foo.* pattern, bar.* should still work
      connection.punsubscribe("foo.*");
      value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertThat(value).isEqualTo("punsubscribed-foo.*-1");

      redis.publish("foo.1", "should-not-arrive");
      value = handOffQueue.poll(1, TimeUnit.SECONDS);
      assertThat(value).isNull();

      redis.publish("bar.1", "still-works");
      value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertThat(value).isEqualTo("pmessage-bar.*-bar.1-still-works");

      // Unsubscribe from remaining pattern
      connection.punsubscribe("bar.*");
      value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertThat(value).isEqualTo("punsubscribed-bar.*-0");

      redis.publish("bar.1", "should-not-arrive");
      value = handOffQueue.poll(1, TimeUnit.SECONDS);
      assertThat(value).isNull();
   }

   @Test
   public void testPSubscribeTwoClients() throws InterruptedException {
      RedisPubSubCommands<String, String> client1 = createPubSubConnection();
      BlockingQueue<String> queue1 = addPubSubListener(client1);

      RedisPubSubCommands<String, String> client2 = createPubSubConnection();
      BlockingQueue<String> queue2 = addPubSubListener(client2);

      // Both clients subscribe to the same pattern
      client1.psubscribe("chan.*");
      assertPSubscription(queue1, "chan.*");

      client2.psubscribe("chan.*");
      assertPSubscription(queue2, "chan.*");

      RedisCommands<String, String> redis = redisConnection.sync();
      redis.publish("chan.foo", "hello");

      // Both clients should receive the message
      String value1 = queue1.poll(10, TimeUnit.SECONDS);
      assertThat(value1).isEqualTo("pmessage-chan.*-chan.foo-hello");

      String value2 = queue2.poll(10, TimeUnit.SECONDS);
      assertThat(value2).isEqualTo("pmessage-chan.*-chan.foo-hello");

      client1.punsubscribe("chan.*");
      client2.punsubscribe("chan.*");
   }

   @Test
   public void testPUnsubscribeWithoutArguments() throws InterruptedException {
      RedisPubSubCommands<String, String> connection = createPubSubConnection();
      BlockingQueue<String> handOffQueue = addPubSubListener(connection);

      int listenersBefore = getListeners(cache).size();

      // Subscribe to multiple patterns
      connection.psubscribe("chan1.*", "chan2.*", "chan3.*");
      assertPSubscription(handOffQueue, "chan1.*", "chan2.*", "chan3.*");

      assertThat(getListeners(cache)).hasSize(listenersBefore + 3);

      // Unsubscribe from all patterns without arguments
      connection.punsubscribe();

      // Punsubscribe notifications (order may vary)
      for (int i = 0; i < 3; ++i) {
         String value = handOffQueue.poll(10, TimeUnit.SECONDS);
         assertThat(value).withFailMessage("Didn't receive punsubscribe notification").isNotNull();
         assertThat(value).startsWith("punsubscribed-");
      }

      eventually(() -> getListeners(cache).size() == listenersBefore);

      // Confirm no more messages are delivered
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.publish("chan1.hi", "nope");
      redis.publish("chan2.hi", "nope");
      redis.publish("chan3.hi", "nope");
      String value = handOffQueue.poll(1, TimeUnit.SECONDS);
      assertThat(value).isNull();
   }

   @Test
   public void testPUnsubscribeFromNonSubscribedPatterns() throws InterruptedException {
      RedisPubSubCommands<String, String> connection = createPubSubConnection();
      BlockingQueue<String> handOffQueue = addPubSubListener(connection);

      // First subscribe to something to enter subscriber mode
      connection.subscribe("dummy");
      assertSubscription(handOffQueue, "dummy");

      // Punsubscribe from patterns we never subscribed to
      connection.punsubscribe("foo.*", "bar.*", "quux.*");

      // Should get punsubscribe confirmations with count reflecting remaining subs
      for (String pattern : new String[] { "foo.*", "bar.*", "quux.*" }) {
         String value = handOffQueue.poll(10, TimeUnit.SECONDS);
         assertThat(value).withFailMessage("Didn't receive punsubscribe notification for " + pattern).isNotNull();
         assertThat(value).startsWith("punsubscribed-" + pattern + "-");
      }

      connection.unsubscribe("dummy");
   }

   @Test
   public void testNumPatWithPatterns() throws Exception {
      RedisCommands<String, String> redis = redisConnection.sync();
      assertThat(redis.pubsubNumpat()).isZero();

      RedisPubSubCommands<String, String> client1 = createPubSubConnection();
      BlockingQueue<String> queue1 = addPubSubListener(client1);

      RedisPubSubCommands<String, String> client2 = createPubSubConnection();
      BlockingQueue<String> queue2 = addPubSubListener(client2);

      // Client 1 subscribes to foo* and bar*
      client1.psubscribe("foo*", "bar*");
      assertPSubscription(queue1, "foo*", "bar*");

      // Client 2 subscribes to foo* and baz*
      client2.psubscribe("foo*", "baz*");
      assertPSubscription(queue2, "foo*", "baz*");

      // NUMPAT should count unique patterns: foo*, bar*, baz* = 3
      assertThat(redis.pubsubNumpat()).isEqualTo(3);

      client1.punsubscribe("foo*", "bar*");
      client2.punsubscribe("foo*", "baz*");

      // Drain punsubscribe notifications
      for (int i = 0; i < 2; i++) queue1.poll(10, TimeUnit.SECONDS);
      for (int i = 0; i < 2; i++) queue2.poll(10, TimeUnit.SECONDS);

      eventually(() -> redis.pubsubNumpat() == 0);
   }

   @Test
   public void testMixSubscribeAndPSubscribe() throws InterruptedException {
      RedisPubSubCommands<String, String> connection = createPubSubConnection();
      BlockingQueue<String> handOffQueue = addPubSubListener(connection);

      // Subscribe to exact channel and a pattern that matches it
      connection.subscribe("foo.bar");
      assertSubscription(handOffQueue, "foo.bar");

      connection.psubscribe("foo.*");
      assertPSubscription(handOffQueue, 2, "foo.*");

      RedisCommands<String, String> redis = redisConnection.sync();
      redis.publish("foo.bar", "hello");

      // Should receive both a message and a pmessage
      String value1 = handOffQueue.poll(10, TimeUnit.SECONDS);
      String value2 = handOffQueue.poll(10, TimeUnit.SECONDS);

      assertThat(value1).isNotNull();
      assertThat(value2).isNotNull();

      // One should be message, the other pmessage (order may vary)
      assertThat(new String[] { value1, value2 }).containsExactlyInAnyOrder(
            "message-foo.bar-hello",
            "pmessage-foo.*-foo.bar-hello"
      );

      connection.unsubscribe("foo.bar");
      connection.punsubscribe("foo.*");
   }

   @Test
   public void testPSubscribeIdempotent() throws InterruptedException {
      RedisPubSubCommands<String, String> connection = createPubSubConnection();
      BlockingQueue<String> handOffQueue = addPubSubListener(connection);

      // Subscribe to the same pattern multiple times
      connection.psubscribe("test.*");
      assertPSubscription(handOffQueue, "test.*");

      connection.psubscribe("test.*");
      // Second subscription should still give a psubscribe confirmation
      String value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertThat(value).isNotNull();
      assertThat(value).startsWith("psubscribed-test.*-");

      // Publish a message - should only receive one copy
      RedisCommands<String, String> redis = redisConnection.sync();
      redis.publish("test.hello", "world");

      value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertThat(value).isEqualTo("pmessage-test.*-test.hello-world");

      // No duplicate
      value = handOffQueue.poll(1, TimeUnit.SECONDS);
      assertThat(value).isNull();

      connection.punsubscribe("test.*");
   }

   @Test(dataProvider = "booleans")
   public void testPSubUnsubDisconnect(boolean quit) throws InterruptedException {
      int listenersBefore = getListeners(cache).size();

      RedisPubSubCommands<String, String> connection = createPubSubConnection();
      BlockingQueue<String> handOffQueue = addPubSubListener(connection);

      // Subscribe to patterns
      connection.psubscribe("chan1.*", "chan2.*");
      assertPSubscription(handOffQueue, "chan1.*", "chan2.*");

      assertThat(getListeners(cache)).hasSize(listenersBefore + 2);

      if (quit) {
         connection.getStatefulConnection().close();

         eventually(() -> getListeners(cache).size() == listenersBefore);
         assertThat(getListeners(cache)).hasSize(listenersBefore);
      } else {
         connection.punsubscribe();

         for (int i = 0; i < 2; ++i) {
            String value = handOffQueue.poll(10, TimeUnit.SECONDS);
            assertThat(value).withFailMessage("Didn't receive punsubscribe notification").isNotNull();
            assertThat(value).startsWith("punsubscribed-");
         }

         eventually(() -> getListeners(cache).size() == listenersBefore);
         assertThat(connection.ping()).isEqualTo(PONG);
      }
   }

   protected RedisPubSubCommands<String, String> createPubSubConnection() {
      return client.connectPubSub().sync();
   }

   private void assertSubscription(BlockingQueue<String> queue, String ... channels) throws InterruptedException {
      int i = 1;
      for (String channel : channels) {
         String value = queue.poll(10, TimeUnit.SECONDS);
         assertThat(value).isEqualTo(String.format("subscribed-%s-%d", channel, i++));
      }
   }

   private void assertPSubscription(BlockingQueue<String> queue, String ... patterns) throws InterruptedException {
      assertPSubscription(queue, 1, patterns);
   }

   private void assertPSubscription(BlockingQueue<String> queue, int startCount, String ... patterns) throws InterruptedException {
      int i = startCount;
      for (String pattern : patterns) {
         String value = queue.poll(10, TimeUnit.SECONDS);
         assertThat(value).isEqualTo(String.format("psubscribed-%s-%d", pattern, i++));
      }
   }

   private void assertUnsubscribe(BlockingQueue<String> queue, String ... channels) throws  InterruptedException {
      int i = channels.length;
      for (String channel : channels) {
         String value = queue.poll(10, TimeUnit.SECONDS);
         assertThat(value).isEqualTo(String.format("unsubscribed-%s-%d", channel, --i));
      }
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
         public void message(String pattern, String channel, String message) {
            log.tracef("Received pmessage pattern %s channel %s of %s", pattern, channel, message);
            handOffQueue.add("pmessage-" + pattern + "-" + channel + "-" + message);
         }

         @Override
         public void subscribed(String channel, long count) {
            log.tracef("Subscribed to %s with %s", channel, count);
            handOffQueue.add("subscribed-" + channel + "-" + count);
         }

         @Override
         public void psubscribed(String pattern, long count) {
            log.tracef("PSubscribed to %s with %s", pattern, count);
            handOffQueue.add("psubscribed-" + pattern + "-" + count);
         }

         @Override
         public void unsubscribed(String channel, long count) {
            log.tracef("Unsubscribed to %s with %s", channel, count);
            handOffQueue.add("unsubscribed-" + channel + "-" + count);
         }

         @Override
         public void punsubscribed(String pattern, long count) {
            log.tracef("PUnsubscribed from %s with %s", pattern, count);
            handOffQueue.add("punsubscribed-" + pattern + "-" + count);
         }
      });

      return handOffQueue;
   }
}
