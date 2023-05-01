package org.infinispan.server.resp;

import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.infinispan.Cache;
import org.infinispan.distribution.DistributionTestHelper;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.server.resp.test.CommonRespTests;
import org.infinispan.test.Mocks;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.infinispan.server.resp.test.RespTestingUtil.OK;
import static org.infinispan.server.resp.test.RespTestingUtil.PONG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;

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
}
