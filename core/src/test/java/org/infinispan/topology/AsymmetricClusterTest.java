package org.infinispan.topology;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.jgroups.protocols.DISCARD;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "topology.AsymmetricClusterTest")
@CleanupAfterMethod
public class AsymmetricClusterTest extends MultipleCacheManagersTest {

   public static final String CACHE_NAME = "testCache";
   private ConfigurationBuilder localConfig;
   private ConfigurationBuilder clusteredConfig;

   DISCARD d1, d2;

   @Override
   protected void createCacheManagers() throws Throwable {
      localConfig = new ConfigurationBuilder();
      clusteredConfig = new ConfigurationBuilder();
      clusteredConfig.clustering().cacheMode(CacheMode.REPL_SYNC).stateTransfer().timeout(30, TimeUnit.SECONDS);

      for (int i = 0; i < 2; i++) addClusterEnabledCacheManager(localConfig, new TransportFlags().withFD(true));

      d1 = TestingUtil.getDiscardForCache(manager(0));
      d2 = TestingUtil.getDiscardForCache(manager(1));
   }

   public void testCrashAndRestartOnlyMember() throws Exception {
      testRestartOnlyMember(true);
   }

   public void testStopAndRestartOnlyMember() throws Exception {
      testRestartOnlyMember(false);
   }

   private void testRestartOnlyMember(boolean crash) {
      // The coordinator stays up throughout the test, but the cache only runs on node 1 and then 2
      manager(1).defineConfiguration(CACHE_NAME, clusteredConfig.build());

      manager(1).getCache(CACHE_NAME);

      if (crash) {
         d2.setDiscardAll(true);
      }
      manager(1).stop();

      TestingUtil.blockUntilViewsReceived(30000, false, manager(0));

      addClusterEnabledCacheManager(clusteredConfig, new TransportFlags().withFD(true));

      manager(2).getCache(CACHE_NAME);
   }

   public void testCoordinatorCrashesDuringJoin() {
      d2.setDiscardAll(true);

      manager(1).defineConfiguration(CACHE_NAME, clusteredConfig.build());
      fork((Callable<Object>) () -> cache(1, CACHE_NAME));

      TestingUtil.blockUntilViewsReceived(30000, false, manager(0));
      TestingUtil.blockUntilViewsReceived(30000, false, manager(1));

      TestingUtil.waitForNoRebalance(cache(1, CACHE_NAME));
   }
}
