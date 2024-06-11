package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.client.hotrod.test.InternalRemoteCacheManager;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.stats.Stats;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Test that statistics are updated properly when a client reads a key from a non-owner.
 *
 * @author Dan Berindei
 * @since 9.4
 */
@Test(groups = "functional", testName = "client.hotrod.HotRodNonOwnerStatisticsTest")
public class HotRodNonOwnerStatisticsTest extends MultipleCacheManagersTest {
   private HotRodServer hotRodServer;
   private RemoteCacheManager remoteCacheManager;
   private RemoteCache<Object, Object> remoteCache;

   @Override
   protected void createCacheManagers() throws Throwable {
      // Add 3 new nodes, but only start one server, so the client only has one connection
      newCacheManager();
      newCacheManager();
      newCacheManager();

      hotRodServer = startHotRodServer(cacheManagers.get(0));

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder
            clientBuilder = HotRodClientTestingUtil.newRemoteConfigurationBuilder(hotRodServer);
      clientBuilder.statistics().enable();
      // We only need one connection, avoid topology updates
      clientBuilder.clientIntelligence(ClientIntelligence.BASIC);

      remoteCacheManager = new InternalRemoteCacheManager(clientBuilder.build());
      remoteCache = remoteCacheManager.getCache();
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      remoteCacheManager.stop();
      hotRodServer.stop();

      super.destroy();
   }

   private void newCacheManager() {
      ConfigurationBuilder cfg = hotRodCacheConfiguration();
      cfg.statistics().enable();
      cfg.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(2);
      GlobalConfigurationBuilder globalCfg = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalCfg.jmx().mBeanServerLookup(TestMBeanServerLookup.create()).enable();
      addClusterEnabledCacheManager(globalCfg, cfg);
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      super.clearContent();

      Stats cacheStats = advancedCache(0).getStats();
      cacheStats.reset();
      remoteCache.clientStatistics().resetStatistics();
      log.debugf("Stats reset on %s", address(0));
   }

   public void testNonPrimaryGetStats() {
      // We want at least one key for which cache(0) is primary/backup/non-owner
      // 10 keys do not guarantee that 100%, but it's good enough
      int keyCount = 10;
      for (int i = 0; i < keyCount; i++) {
         remoteCache.put("k" + i, "v" + i);

         remoteCache.get("k" + i);
         remoteCache.get("not_presented" + i);
      }
      assertEquals(keyCount, (int) remoteCache.serverStatistics().getIntStatistic(ServerStatistics.HITS));
      assertEquals(keyCount, (int) remoteCache.serverStatistics().getIntStatistic(ServerStatistics.MISSES));
   }
}
