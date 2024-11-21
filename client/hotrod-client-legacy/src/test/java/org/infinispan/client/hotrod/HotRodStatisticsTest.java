package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "client.hotrod.HotRodStatisticsTest")
public class HotRodStatisticsTest extends AbstractInfinispanTest {

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   private HotRodServer hotrodServer;
   private CacheContainer cacheContainer;
   private RemoteCacheManager rcm;
   private RemoteCache<String, String> remoteCache;
   private long startTime;

   @BeforeMethod
   protected void setup() throws Exception {
      ConfigurationBuilder cfg = hotRodCacheConfiguration();
      cfg.statistics().enable();
      GlobalConfigurationBuilder globalCfg = GlobalConfigurationBuilder.defaultClusteredBuilder();
      TestCacheManagerFactory.configureJmx(globalCfg, getClass().getSimpleName(), mBeanServerLookup);
      globalCfg.metrics().accurateSize(true);
      cacheContainer = TestCacheManagerFactory.createClusteredCacheManager(globalCfg, cfg);

      hotrodServer = HotRodClientTestingUtil.startHotRodServer((EmbeddedCacheManager) cacheContainer);
      startTime = System.currentTimeMillis();
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(hotrodServer.getPort());
      clientBuilder.statistics().enable();
      rcm = new RemoteCacheManager(clientBuilder.build());
      remoteCache = rcm.getCache();
   }

   @AfterMethod
   void tearDown() {
      TestingUtil.killCacheManagers(cacheContainer);
      killRemoteCacheManager(rcm);
      killServers(hotrodServer);
      hotrodServer = null;
   }

   public void testAllStatsArePresent() {
      ServerStatistics serverStatistics = remoteCache.serverStatistics();
      Map<String, String> statsMap = serverStatistics.getStatsMap();
      assertEquals(statsMap.get(ServerStatistics.STORES), "0");
      assertEquals(statsMap.get(ServerStatistics.CURRENT_NR_OF_ENTRIES), "0");
      assertEquals(statsMap.get(ServerStatistics.HITS),"0");
      assertEquals(statsMap.get(ServerStatistics.MISSES),"0");
      assertEquals(statsMap.get(ServerStatistics.REMOVE_HITS),"0");
      assertEquals(statsMap.get(ServerStatistics.REMOVE_MISSES),"0");
      assertEquals(statsMap.get(ServerStatistics.RETRIEVALS),"0");
      assertEquals(statsMap.get(ServerStatistics.APPROXIMATE_ENTRIES), "0");
      assertEquals(statsMap.get(ServerStatistics.APPROXIMATE_ENTRIES_UNIQUE), "0");
      assertEquals(0, remoteCache.size());
      assertTrue(remoteCache.isEmpty());

      Integer number = serverStatistics.getIntStatistic(ServerStatistics.TIME_SINCE_START);
      assertTrue(number >= 0);
   }

   public void testStoresAndEntries() {
      assertEquals(0, remoteCache.size());
      assertTrue(remoteCache.isEmpty());
      remoteCache.put("a","v");
      ServerStatistics serverStatistics = remoteCache.serverStatistics();
      assertIntStatistic(1, serverStatistics, ServerStatistics.STORES);
      assertEquals(1, remoteCache.clientStatistics().getRemoteStores());
      assertIntStatistic(1, serverStatistics, ServerStatistics.CURRENT_NR_OF_ENTRIES);
      assertIntStatistic(1, serverStatistics, ServerStatistics.APPROXIMATE_ENTRIES);
      assertIntStatistic(1, serverStatistics, ServerStatistics.APPROXIMATE_ENTRIES_UNIQUE);
      assertEquals(1, remoteCache.size());
      assertFalse(remoteCache.isEmpty());

      remoteCache.put("a2","v2");
      serverStatistics = remoteCache.serverStatistics();
      assertIntStatistic(2, serverStatistics, ServerStatistics.STORES);
      assertEquals(2, remoteCache.clientStatistics().getRemoteStores());
      assertIntStatistic(2, serverStatistics, ServerStatistics.CURRENT_NR_OF_ENTRIES);
      assertIntStatistic(2, serverStatistics, ServerStatistics.APPROXIMATE_ENTRIES);
      assertIntStatistic(2, serverStatistics, ServerStatistics.APPROXIMATE_ENTRIES_UNIQUE);
      assertEquals(2,remoteCache.size());
      assertFalse(remoteCache.isEmpty());

      remoteCache.put("a2","v3");
      serverStatistics = remoteCache.serverStatistics();
      assertIntStatistic(3, serverStatistics, ServerStatistics.STORES);
      assertEquals(3, remoteCache.clientStatistics().getRemoteStores());
      assertIntStatistic(2, serverStatistics, ServerStatistics.CURRENT_NR_OF_ENTRIES);
      assertIntStatistic(2, serverStatistics, ServerStatistics.APPROXIMATE_ENTRIES);
      assertIntStatistic(2, serverStatistics, ServerStatistics.APPROXIMATE_ENTRIES_UNIQUE);
      assertEquals(2, remoteCache.size());
      assertFalse(remoteCache.isEmpty());
   }

   public void testHitsAndMisses() {
      remoteCache.get("a");
      assertIntStatistic(0, remoteCache.serverStatistics(), ServerStatistics.HITS);
      assertEquals(0, remoteCache.clientStatistics().getRemoteHits());
      assertIntStatistic(1, remoteCache.serverStatistics(), ServerStatistics.MISSES);
      assertEquals(1, remoteCache.clientStatistics().getRemoteMisses());
      remoteCache.put("a","v");
      assertIntStatistic(0, remoteCache.serverStatistics(), ServerStatistics.HITS);
      assertEquals(0, remoteCache.clientStatistics().getRemoteHits());
      assertIntStatistic(1, remoteCache.serverStatistics(), ServerStatistics.MISSES);
      assertEquals(1, remoteCache.clientStatistics().getRemoteMisses());
      remoteCache.get("a");
      assertIntStatistic(1, remoteCache.serverStatistics(), ServerStatistics.HITS);
      assertEquals(1, remoteCache.clientStatistics().getRemoteHits());
      assertIntStatistic(1, remoteCache.serverStatistics(), ServerStatistics.MISSES);
      assertEquals(1, remoteCache.clientStatistics().getRemoteMisses());
      remoteCache.get("a");
      remoteCache.get("a");
      remoteCache.get("a");
      assertIntStatistic(4, remoteCache.serverStatistics(), ServerStatistics.HITS);
      assertEquals(4, remoteCache.clientStatistics().getRemoteHits());
      assertIntStatistic(1, remoteCache.serverStatistics(), ServerStatistics.MISSES);
      assertEquals(1, remoteCache.clientStatistics().getRemoteMisses());
   }

   public void testRemoveHitsAndMisses() {
      remoteCache.remove("a");
      assertIntStatistic(0, remoteCache.serverStatistics(), ServerStatistics.REMOVE_HITS);
      assertIntStatistic(1, remoteCache.serverStatistics(), ServerStatistics.REMOVE_MISSES);
      remoteCache.put("a","v");
      remoteCache.remove("a");
      assertIntStatistic(1, remoteCache.serverStatistics(), ServerStatistics.REMOVE_HITS);
      assertIntStatistic(1, remoteCache.serverStatistics(), ServerStatistics.REMOVE_MISSES);
      remoteCache.put("a","v");
      remoteCache.put("b","v");
      remoteCache.put("c","v");

      remoteCache.remove("a");
      remoteCache.remove("b");
      remoteCache.remove("c");
      assertIntStatistic(4, remoteCache.serverStatistics(), ServerStatistics.REMOVE_HITS);
      assertIntStatistic(1, remoteCache.serverStatistics(), ServerStatistics.REMOVE_MISSES);
   }

   public void testNumberOfEntriesAfterClear() {
      ServerStatistics serverStatistics = remoteCache.serverStatistics();
      assertIntStatistic(0, serverStatistics, ServerStatistics.CURRENT_NR_OF_ENTRIES);
      assertIntStatistic(0, serverStatistics, ServerStatistics.APPROXIMATE_ENTRIES);
      assertIntStatistic(0, serverStatistics, ServerStatistics.APPROXIMATE_ENTRIES_UNIQUE);

      remoteCache.put("k", "v");
      remoteCache.put("k2", "v");
      serverStatistics = remoteCache.serverStatistics();
      assertIntStatistic(2, serverStatistics, ServerStatistics.CURRENT_NR_OF_ENTRIES);
      assertIntStatistic(2, serverStatistics, ServerStatistics.APPROXIMATE_ENTRIES);
      assertIntStatistic(2, serverStatistics, ServerStatistics.APPROXIMATE_ENTRIES_UNIQUE);

      remoteCache.clear();
      serverStatistics = remoteCache.serverStatistics();
      assertIntStatistic(0, serverStatistics, ServerStatistics.CURRENT_NR_OF_ENTRIES);
      assertIntStatistic(0, serverStatistics, ServerStatistics.APPROXIMATE_ENTRIES);
      assertIntStatistic(0, serverStatistics, ServerStatistics.APPROXIMATE_ENTRIES_UNIQUE);
   }

   private void assertIntStatistic(int expected, ServerStatistics serverStatistics, String statName) {
      assertEquals((Integer) expected, serverStatistics.getIntStatistic(statName));
   }
}
