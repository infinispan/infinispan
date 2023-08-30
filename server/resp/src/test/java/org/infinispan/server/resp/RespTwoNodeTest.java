package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.resp.test.RespTestingUtil.OK;
import static org.infinispan.server.resp.test.RespTestingUtil.PONG;
import static org.infinispan.test.TestingUtil.k;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.assertj.core.api.SoftAssertions;
import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.distribution.DistributionTestHelper;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.server.resp.test.CommonRespTests;
import org.infinispan.test.Mocks;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.testng.annotations.Test;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.pubsub.RedisPubSubAdapter;
import io.lettuce.core.pubsub.api.sync.RedisPubSubCommands;

@Test(groups = "functional", testName = "server.resp.RespTwoNodeTest")
public class RespTwoNodeTest extends BaseMultipleRespTest {

   public void testConcurrentOperations() throws ExecutionException, InterruptedException, TimeoutException {
      CheckPoint checkPoint = new CheckPoint();
      checkPoint.triggerForever(Mocks.AFTER_RELEASE);

      String blockedKey = "foo";
      // We block the non owner, so we know the primary owner of the data's netty
      // thread isn't blocked on accident
      Cache<String, String> nonOwner = DistributionTestHelper.getFirstBackupOwner(blockedKey,
            caches(server1.getConfiguration().defaultCacheName()));

      var original = Mocks.blockingMock(checkPoint, ClusteringDependentLogic.class, nonOwner,
            (stubber, clusteringDependentLogic) -> stubber.when(clusteringDependentLogic).commitEntry(any(), any(),
                  any(), any(), anyBoolean()));
      RedisAsyncCommands<String, String> redis = redisConnection1.async();
      try {
         RedisFuture<String> futureSet = redis.set(blockedKey, "bar");

         checkPoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS);

         RedisFuture<String> futurePing = redis.ping();
         RedisFuture<List<Object>> futureCommand = redis.command();

         checkPoint.triggerForever(Mocks.BEFORE_RELEASE);

         String getResponse = futurePing.get(10, TimeUnit.SECONDS);
         assertThat(futureSet.get(10, TimeUnit.SECONDS)).isEqualTo(OK);
         assertThat(getResponse).isEqualTo(PONG);
         List<Object> results = futureCommand.get(10, TimeUnit.SECONDS);
         assertThat(results).hasSizeGreaterThan(10);
      } finally {
         TestingUtil.replaceComponent(nonOwner, ClusteringDependentLogic.class, original, true);
      }

      RedisFuture<String> getFuture = redis.get(blockedKey);
      assertThat(getFuture.get(10, TimeUnit.SECONDS)).isEqualTo("bar");
   }

   public void testPipeline() throws ExecutionException, InterruptedException, TimeoutException {
      CommonRespTests.testPipeline(redisConnection1);
   }

   public void testPfaddMultipleServers() throws Exception {
      String k0 = k(0);
      CompletableFuture<Long> c1 = redisConnection1.async().pfadd(k0, "el1", "el2").toCompletableFuture();
      CompletableFuture<Long> c2 = redisConnection2.async().pfadd(k0, "el3", "el4").toCompletableFuture();

      CompletableFutures.await(c1, 10, TimeUnit.SECONDS);
      CompletableFutures.await(c2, 10, TimeUnit.SECONDS);

      assertThat(c1.get()).isEqualTo(1L);
      assertThat(c2.get()).isEqualTo(1L);

      String k1 = k(1);
      RedisCommands<String, String> syncC1 = redisConnection1.sync();
      RedisCommands<String, String> syncC2 = redisConnection1.sync();
      for (int i = 0; i < 193; i++) {
         if ((i & 1) == 1) {
            assertThat(syncC1.pfadd(k1, "el-" + i)).isEqualTo(1L);
         } else {
            assertThat(syncC2.pfadd(k1, "el-" + i)).isEqualTo(1L);
         }
      }

      // From this point on, it is using the probabilistic estimation.
      SoftAssertions sa = new SoftAssertions();
      for (int i = 0; i < 831; i++) {
         if ((i & 1) == 1) {
            sa.assertThat(syncC1.pfadd(k1, "hello-" + i)).isEqualTo(1L);
         } else {
            sa.assertThat(syncC2.pfadd(k1, "hello-" + i)).isEqualTo(1L);
         }
      }

      assertThat(syncC1.pfadd(k1, "el-0", "hello-1", "hello-2")).isEqualTo(0L);
      assertThat(syncC2.pfadd(k1, "el-0", "hello-1", "hello-2")).isEqualTo(0L);
      assertThat(sa.errorsCollected()).hasSize(16);
      // TODO: Verify cardinality ISPN-14676
   }

   @Test
   public void testPubSub() throws InterruptedException {
      RedisPubSubCommands<String, String> connection = createPubSubConnection();
      BlockingQueue<String> handOffQueue = addPubSubListener(connection);
      // Subscribe to some channels
      List<String> channels = Arrays.asList("channel2", "test", "channel");
      connection.subscribe("channel2", "test");
      String value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertThat(value).isEqualTo("subscribed-channel2-1");
      value = handOffQueue.poll(10, TimeUnit.SECONDS);
      assertThat(value).isEqualTo("subscribed-test-2");

      // Send a message to confirm it is properly listening
      RedisCommands<String, String> redis = redisConnection2.sync();
      redis.publish("channel2", "boomshakayaka");
      value = handOffQueue.poll(10, TimeUnit.SECONDS);
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

   protected RedisPubSubCommands<String, String> createPubSubConnection() {
      return client2.connectPubSub().sync();
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

   @Test
   public void testBlpopTwoPushTwoListeners() throws InterruptedException, ExecutionException {
      var redisCommmand1 = client1.connect().async();
      var redisCommmand2 = client2.connect().async();
      redisConnection1.sync().rpush("keyA", "val1");
      redisConnection2.sync().rpush("keyA", "val2");
      var cf1 = redisCommmand1.blpop(0, "keyA");
      var cf2 = redisCommmand2.blpop(0, "keyA");
      assertThat(Arrays.asList(cf1.get().getValue(), cf2.get().getValue())).containsExactlyInAnyOrder("val1","val2");
   }



   @Test
   public void testBlpopTwoListenersTwoPush() throws InterruptedException, ExecutionException {
      var redisCommmand1 = client1.connect().async();
      var redisCommmand2 = client2.connect().async();
      var cf1 = redisCommmand1.blpop(0, "keyA");
      var cf2 = redisCommmand2.blpop(0, "keyA");
      redisConnection1.sync().rpush("keyA", "val1");
      redisConnection2.sync().rpush("keyA", "val2");
      assertThat(Arrays.asList(cf1.get().getValue(), cf2.get().getValue())).containsExactlyInAnyOrder("val1","val2");
   }

   @Test
   public void testBlpopListenerOn1PushOn2() throws InterruptedException, ExecutionException {
      var redisCommmand1 = client1.connect().async();
      var cf1 = redisCommmand1.blpop(0, "keyA");
      redisConnection2.sync().rpush("keyA", "val1","val2");
      assertThat(cf1.get().getValue()).isEqualTo("val1");
   }

   @Test
   public void testBlpopPushOn2ListenerOn1() throws InterruptedException, ExecutionException {
      var redisCommmand1 = client1.connect().async();
      redisConnection2.sync().rpush("keyA", "val1","val2");
      var cf1 = redisCommmand1.blpop(0, "keyA");
      assertThat(cf1.get().getValue()).isEqualTo("val1");
   }

   @Test
   public void testBlpopMixedCase() throws InterruptedException, ExecutionException {
      var redisCommmand1 = client1.connect().async();
      var redisCommmand2 = client2.connect().async();
      var cf1 = redisCommmand1.blpop(0, "keyA");
      redisConnection1.sync().rpush("keyA", "val1");
      redisConnection2.sync().rpush("keyA", "val2");
      var cf2 = redisCommmand2.blpop(0, "keyA");
      assertThat(Arrays.asList(cf1.get().getValue(), cf2.get().getValue())).containsExactlyInAnyOrder("val1","val2");
   }

}
