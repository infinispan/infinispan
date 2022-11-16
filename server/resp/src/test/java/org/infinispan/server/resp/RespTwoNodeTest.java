package org.infinispan.server.resp;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.infinispan.Cache;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.distribution.DistributionTestHelper;
import org.infinispan.globalstate.ConfigurationStorage;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.server.resp.configuration.RespServerConfiguration;
import org.infinispan.server.resp.configuration.RespServerConfigurationBuilder;
import org.infinispan.server.resp.test.RespTestingUtil;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.server.resp.test.RespTestingUtil.createClient;
import static org.infinispan.server.resp.test.RespTestingUtil.killClient;
import static org.infinispan.server.resp.test.RespTestingUtil.killServer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "functional", testName = "server.resp.RespTwoNodeTest")
public class RespTwoNodeTest extends MultipleCacheManagersTest {
   protected RedisClient client1;
   protected RespServer server1;
   protected RespServer server2;
   protected StatefulRedisConnection<String, String> redisConnection1;
   protected static final int timeout = 60;

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder cacheBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      GlobalConfigurationBuilder gcb1 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      String stateDirectory1 = tmpDirectory(this.getClass().getSimpleName(), this.getClass().getSimpleName() + "_1");
      Util.recursiveFileRemove(stateDirectory1);
      gcb1.globalState().enable().persistentLocation(stateDirectory1).configurationStorage(ConfigurationStorage.OVERLAY);
      gcb1.globalState().sharedPersistentLocation(stateDirectory1);
      gcb1.transport().defaultTransport();

      GlobalConfigurationBuilder gcb2 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      String stateDirectory2 = tmpDirectory(this.getClass().getSimpleName(), this.getClass().getSimpleName() + "_2");
      Util.recursiveFileRemove(stateDirectory2);
      gcb2.globalState().enable().persistentLocation(stateDirectory2).configurationStorage(ConfigurationStorage.OVERLAY);
      gcb2.globalState().sharedPersistentLocation(stateDirectory2);
      gcb2.transport().defaultTransport();

      createCluster(Arrays.asList(gcb1, gcb2), cacheBuilder);
      waitForClusterToForm();
   }

   @BeforeClass(alwaysRun = true)
   @Override
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass();
      RespServerConfiguration serverConfiguration = serverConfiguration(0).build();
      server1 = RespTestingUtil.startServer(cacheManagers.get(0), serverConfiguration);
      server2 = RespTestingUtil.startServer(cacheManagers.get(1), serverConfiguration(1).build());
      client1 = createClient(30000, serverConfiguration.port());
      redisConnection1 = client1.connect();
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      log.debug("Test finished, close resp server");
      killClient(client1);
      killServer(server1);
      killServer(server2);
      super.destroy();
   }

   protected RespServerConfigurationBuilder serverConfiguration(int offset) {
      String serverName = TestResourceTracker.getCurrentTestShortName();
      return new RespServerConfigurationBuilder().name(serverName)
            .host(RespTestingUtil.HOST)
            .port(RespTestingUtil.port() + offset);
   }

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
         assertEquals("OK", futureSet.get(10, TimeUnit.SECONDS));
         assertEquals("PONG", getResponse);
         List<Object> results = futureCommand.get(10, TimeUnit.SECONDS);
         assertTrue("Results were: " + results, results.size() > 10);
      } finally {
         TestingUtil.replaceComponent(nonOwner, ClusteringDependentLogic.class, original, true);
      }

      RedisFuture<String> getFuture = redis.get(blockedKey);
      assertEquals("bar", getFuture.get(10, TimeUnit.SECONDS));
   }
}
