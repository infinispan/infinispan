package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.resp.test.RespTestingUtil.OK;
import static org.infinispan.server.resp.test.RespTestingUtil.PONG;
import static org.infinispan.test.TestingUtil.v;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.assertj.core.api.SoftAssertions;
import org.infinispan.Cache;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.distribution.DistributionTestHelper;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.server.resp.test.CommonRespTests;
import org.infinispan.test.Mocks;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.testng.annotations.Test;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.LMPopArgs;
import io.lettuce.core.LMoveArgs;
import io.lettuce.core.Range;
import io.lettuce.core.RedisCommandExecutionException;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.SortArgs;
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

   public void testRename() {
      RedisCommands<String, String> r0 = redisConnection1.sync();
      RedisCommands<String, String> r1 = redisConnection2.sync();
      String srcKey = getStringKeyForCache(respCache(0));
      String dstKey = getStringKeyForCache(respCache(1));
      String val = v();
      r0.set(srcKey, val);
      r0.rename(srcKey, dstKey);
      assertThat(r0.get(dstKey)).isEqualTo(val);
      assertThat(r1.get(dstKey)).isEqualTo(val);

      r1.rename(dstKey, srcKey);
      assertThat(r0.get(srcKey)).isEqualTo(val);
      assertThat(r1.get(srcKey)).isEqualTo(val);

      Exceptions.expectException(RedisCommandExecutionException.class,
            "ERR no such key",
            () -> r0.rename(getStringKeyForCache(respCache(1)), dstKey));
   }

   public void testSimpleScan() {
      int size = 15;

      RedisCommands<String, String> r1 = redisConnection1.sync();
      RedisCommands<String, String> r2 = redisConnection2.sync();

      Set<String> all = new HashSet<>();
      while (all.size() < size) {
         String k = getStringKeyForCache(respCache(0));
         r1.set(k, v());
         all.add(k);
      }

      Set<String> keys = new HashSet<>();
      for (KeyScanCursor<String> cursor = r2.scan();; cursor = r2.scan(cursor)) {
         keys.addAll(cursor.getKeys());
         if (cursor.isFinished())
            break;
      }
      assertTrue(keys.containsAll(all));
   }

   public void testSort() {
      RedisCommands<String, String> r0 = redisConnection1.sync();
      RedisCommands<String, String> r1 = redisConnection2.sync();

      assertThat(r0.sort("not_existing")).isEmpty();
      assertThat(r1.sortReadOnly("not_existing")).isEmpty();

      // Create on node 0 only.
      String numbers = getStringKeyForCache(respCache(0));
      r0.rpush(numbers, "1", "3", "4", "8", "1", "0", "-1", "19", "-22", "3");

      // SORT numbers
      assertThat(r0.sort(numbers)).containsExactly("-22", "-1", "0", "1", "1", "3", "3", "4", "8", "19");
      assertThat(r1.sort(numbers)).containsExactly("-22", "-1", "0", "1", "1", "3", "3", "4", "8", "19");

      // We create the list on node 0.
      String people = getStringKeyForCache(respCache(0));
      String w1 = getStringKeyForCache("w_", respCache(1));
      String w2 = getStringKeyForCache("w_", respCache(1));
      String v1 = w1.replace("w_", ""), v2 = w2.replace("w_", "");
      r0.rpush(people, v1, v2);

      // But we store the weights on node 1.
      r1.set(w1, "1");
      r1.set(w2, "2");

      // SORT people BY w_*
      assertThat(r0.sort(people, SortArgs.Builder.by("w_*"))).containsExactly( v1, v2);
      assertThat(r1.sort(people, SortArgs.Builder.by("w_*"))).containsExactly( v1, v2);

      // We read list from node 0, weights from node 1, and store on node 1.
      String target = getStringKeyForCache(respCache(1));
      assertThat(r0.sortStore(people, SortArgs.Builder.by("w_*"), target)).isEqualTo(2);
   }

   public void testPop() {
      RedisCommands<String, String> r0 = redisConnection1.sync();
      RedisCommands<String, String> r1 = redisConnection2.sync();

      String k = getStringKeyForCache(respCache(0));

      // Create on node 0.
      r0.rpush(k, "v1", "v2", "v3", "v4");

      // First pop with node 1.
      assertThat(r1.lpop(k)).isEqualTo("v1");
      assertThat(r1.rpop(k)).isEqualTo("v4");

      // Then pop with node 0.
      assertThat(r0.lpop(k)).isEqualTo("v2");
      assertThat(r0.rpop(k)).isEqualTo("v3");

      assertThat(r0.rpop(k)).isNull();
      assertThat(r1.rpop(k)).isNull();
   }

   public void testPushX() {
      RedisCommands<String, String> r0 = redisConnection1.sync();
      RedisCommands<String, String> r1 = redisConnection2.sync();

      String k = getStringKeyForCache(respCache(0));

      // Node 1 tries to create with a key owned by 0. No key yet, operation should fail.
      assertThat(r1.rpushx(k, "v0")).isEqualTo(0L);
      r0.rpush(k, "v2");

      // Now node 1 succeed.
      assertThat(r1.rpushx(k, "v1")).isEqualTo(2L);
      assertThat(r1.lpushx(k, "v3")).isEqualTo(3L);

      assertThat(r0.lrange(k, 0, -1)).containsExactly("v3", "v2", "v1");
   }

   public void testLMove() {
      RedisCommands<String, String> r0 = redisConnection1.sync();
      RedisCommands<String, String> r1 = redisConnection2.sync();

      // We have dest and source on different nodes.
      String srcKey0 = getStringKeyForCache(respCache(0));
      String srcKey1 = getStringKeyForCache(respCache(1));
      String dstKey0 = getStringKeyForCache(respCache(0));
      String dstKey1 = getStringKeyForCache(respCache(1));
      String v = v();

      // First, let's create it.
      r0.rpush(srcKey0, v);

      // Then let's move it. Node 1 reads from 0 and write on 1.
      assertThat(r1.lmove(srcKey0, dstKey1, LMoveArgs.Builder.rightRight())).isEqualTo(v);

      // Then let's move only in the same node.
      assertThat(r1.lmove(dstKey1, srcKey1, LMoveArgs.Builder.rightRight())).isEqualTo(v);

      // Now let's move to 0 again. Node 0 reads from 1 and writes on 0.
      assertThat(r0.lmove(srcKey1, dstKey0, LMoveArgs.Builder.rightRight())).isEqualTo(v);

      // And another local move, but now executed by the REMOTE node 1.
      assertThat(r1.lmove(dstKey0, srcKey0, LMoveArgs.Builder.rightRight())).isEqualTo(v);
   }

   public void testLMPop() {
      RedisCommands<String, String> r0 = redisConnection1.sync();
      RedisCommands<String, String> r1 = redisConnection2.sync();

      String k0 = getStringKeyForCache(respCache(0));
      String k1 = getStringKeyForCache(respCache(1));

      r0.rpush(k0, "v1", "v2");
      r1.rpush(k1, "v1", "v2");

      // Node 0 pops from itself.
      assertThat(r0.lmpop(LMPopArgs.Builder.left(), k0))
            .satisfies(kv -> assertThat(kv.getKey()).isEqualTo(k0))
            .satisfies(kv -> assertThat(kv.getValue()).containsExactly("v1"));

      // Node 0 pops from Node 1.
      assertThat(r0.lmpop(LMPopArgs.Builder.left(), k1))
            .satisfies(kv -> assertThat(kv.getKey()).isEqualTo(k1))
            .satisfies(kv -> assertThat(kv.getValue()).containsExactly("v1"));
   }

   public void testLPos() {
      RedisCommands<String, String> r0 = redisConnection1.sync();
      RedisCommands<String, String> r1 = redisConnection2.sync();

      String k0 = getStringKeyForCache(respCache(0));
      String k1 = getStringKeyForCache(respCache(1));
      String nonExistent = getStringKeyForCache(respCache(1));

      r0.rpush(k0, "v1", "v2");
      r1.rpush(k1, "v1", "v2");

      // Node 0 gets from itself.
      assertThat(r0.lpos(k0, "v1")).isEqualTo(0);

      // Node 0 gets from node 1.
      assertThat(r0.lpos(k1, "v2")).isEqualTo(1);

      // Node 0 tries random value from node 1.
      assertThat(r0.lpos(k1, "something")).isNull();

      // Node 0 gets non-existent from node 1.
      assertThat(r0.lpos(nonExistent, "v1")).isNull();
   }

   public void testLSet() {
      RedisCommands<String, String> r0 = redisConnection1.sync();
      RedisCommands<String, String> r1 = redisConnection2.sync();

      String k0 = getStringKeyForCache(respCache(0));
      String k1 = getStringKeyForCache(respCache(1));

      r0.rpush(k0, "v1", "v2");
      r1.rpush(k1, "v1", "v2");

      // Node 0 sets on itself.
      assertThat(r0.lset(k0, 0, "v3")).isEqualTo("OK");

      // Node 0 sets on node 1.
      assertThat(r0.lset(k1, 0, "v3")).isEqualTo("OK");
   }

   public void testSortedSetUnion() {
      RedisCommands<String, String> r0 = redisConnection1.sync();
      RedisCommands<String, String> r1 = redisConnection2.sync();

      assertThat(r0.zunion("not_existing")).isEmpty();

      String src0 = getStringKeyForCache(respCache(0));
      String dest0 = getStringKeyForCache(respCache(0));
      String src1 = getStringKeyForCache(respCache(1));

      // Node 0 insert into 1, and vice-versa.
      r0.zadd(src1, ScoredValue.just(1, "a"), ScoredValue.just(2, "b"));
      r1.zadd(src0, ScoredValue.just(1, "a"), ScoredValue.just(2, "b"));

      // Now node 0 does a zunion with node 1 only.
      assertThat(r0.zunion(src1)).contains("a", "b");

      // Now node 1 executes a union with node 0 and store remotely.
      assertThat(r1.zunionstore(dest0, src1, src0)).isEqualTo(2);
   }

   public void testSortedSetDiff() {
      RedisCommands<String, String> r0 = redisConnection1.sync();
      RedisCommands<String, String> r1 = redisConnection2.sync();

      String src0 = getStringKeyForCache(respCache(0));
      String dest0 = getStringKeyForCache(respCache(0));
      String src1 = getStringKeyForCache(respCache(1));
      String anotherSrc1 = getStringKeyForCache(respCache(1));

      assertThat(r0.zdiff(src0, src1)).isEmpty();

      // Node 0 insert into 1, and vice-versa.
      r0.zadd(src1, ScoredValue.just(1, "a"));
      r1.zadd(src0, ScoredValue.just(2, "b"), ScoredValue.just(3, "c"));

      // Additional values for remote calls.
      r1.zadd(anotherSrc1, ScoredValue.just(4, "d"), ScoredValue.just(5, "e"));

      // Now node 0 gets the diff with node 0 and 1.
      assertThat(r0.zdiff(src0, src1, anotherSrc1)).contains("b", "c");

      System.out.println("OK");
      // Node 1 gets the diff and store on node 0.
      assertThat(r1.zdiffstore(dest0, src0, src1)).isEqualTo(2);
   }

   public void testSortedSetIntersections() {
      RedisCommands<String, String> r0 = redisConnection1.sync();
      RedisCommands<String, String> r1 = redisConnection2.sync();

      String src0 = getStringKeyForCache(respCache(0));
      String src1 = getStringKeyForCache(respCache(1));

      // Adding to one another.
      r0.zadd(src1, ScoredValue.just(1, "a"), ScoredValue.just(2, "b"));
      r1.zadd(src0, ScoredValue.just(2, "b"), ScoredValue.just(3, "c"));

      // Checking intersections.
      assertThat(r1.zinter(src0, src1)).containsOnly("b");
      assertThat(r0.zintercard(src0, src1)).isEqualTo(1);
   }

   public void testSortedSetRanges() {
      RedisCommands<String, String> r0 = redisConnection1.sync();
      RedisCommands<String, String> r1 = redisConnection2.sync();

      String src0 = getStringKeyForCache(respCache(0));
      String src1 = getStringKeyForCache(respCache(1));
      String dest1 = getStringKeyForCache(respCache(1));

      // Adding to one another.
      r0.zadd(src1, ScoredValue.just(1, "a"), ScoredValue.just(2, "b"));
      r1.zadd(src0, ScoredValue.just(2, "b"), ScoredValue.just(3, "c"));

      assertThat(r1.zrange(src0, 0, -1)).containsOnly("b", "c");
      assertThat(r0.zrangestore(dest1, src1, Range.create(0L, -1L))).isEqualTo(2);
   }

   public void testPfaddMultipleServers() throws Exception {
      String k0 = getStringKeyForCache(respCache(0));
      CompletableFuture<Long> c1 = redisConnection1.async().pfadd(k0, "el1", "el2").toCompletableFuture();
      CompletableFuture<Long> c2 = redisConnection2.async().pfadd(k0, "el3", "el4").toCompletableFuture();

      CompletableFutures.await(c1, 10, TimeUnit.SECONDS);
      CompletableFutures.await(c2, 10, TimeUnit.SECONDS);

      assertThat(c1.get()).isEqualTo(1L);
      assertThat(c2.get()).isEqualTo(1L);

      String k1 = getStringKeyForCache(respCache(1));
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


   @Test
   public void testRpushLrange() throws InterruptedException, ExecutionException {
      var redisCmd1 = redisConnection1.sync();
      var redisCmd2 = redisConnection2.sync();

      String keyA = getStringKeyForCache(respCache(0));
      redisCmd1.rpush(keyA, "val1");
      redisCmd2.rpush(keyA, "val2");
      assertThat(redisCmd1.lrange(keyA, 0,-1)).containsExactlyInAnyOrder("val1","val2");
      assertThat(redisCmd2.lrange(keyA, 0,-1)).containsExactlyInAnyOrder("val1","val2");
   }
}
