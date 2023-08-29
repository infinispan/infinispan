package org.infinispan.server.resp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.resp.test.RespTestingUtil.OK;
import static org.infinispan.server.resp.test.RespTestingUtil.PONG;
import static org.infinispan.test.TestingUtil.k;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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

@Test(groups = "functional", testName = "server.resp.RespTwoNodeTest")
public class RespTwoNodeTest extends BaseMultipleRespTest {

   public void testConcurrentOperations() throws ExecutionException, InterruptedException, TimeoutException {
      CheckPoint checkPoint = new CheckPoint();
      checkPoint.triggerForever(Mocks.AFTER_RELEASE);

      String blockedKey = "foo";
      // We block the non owner, so we know the primary owner of the data's netty thread isn't blocked on accident
      Cache<String, String> nonOwner = DistributionTestHelper.getFirstBackupOwner(blockedKey, caches(server1.getConfiguration().defaultCacheName()));

      var original = Mocks.blockingMock(checkPoint, ClusteringDependentLogic.class, nonOwner, (stubber, clusteringDependentLogic) ->
            stubber.when(clusteringDependentLogic).commitEntry(any(), any(), any(), any(), anyBoolean()));
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
}
