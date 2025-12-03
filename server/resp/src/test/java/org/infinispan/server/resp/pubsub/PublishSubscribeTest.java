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
}
