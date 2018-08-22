package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
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

   private HotRodServer hotrodServer;
   private CacheContainer cacheContainer;
   private RemoteCacheManager rcm;
   private RemoteCache remoteCache;
   long startTime;

   @BeforeMethod
   protected void setup() throws Exception {
      ConfigurationBuilder cfg = hotRodCacheConfiguration();
      cfg.jmxStatistics().enable();
      cacheContainer = TestCacheManagerFactory
            .createClusteredCacheManagerEnforceJmxDomain(getClass().getSimpleName(), cfg);

      hotrodServer = HotRodClientTestingUtil.startHotRodServer((EmbeddedCacheManager) cacheContainer);
      startTime = System.currentTimeMillis();
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
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
      assertEquals(statsMap.get(ServerStatistics.TOTAL_NR_OF_ENTRIES),"0");
      assertEquals(0, remoteCache.size());
      assertTrue(remoteCache.isEmpty());

      Integer number = serverStatistics.getIntStatistic(ServerStatistics.TIME_SINCE_START);
      assertTrue(number >= 0);
   }

   public void testStoresAndEntries() {
      assertEquals(0, remoteCache.size());
      assertTrue(remoteCache.isEmpty());
      remoteCache.put("a","v");
      assertEquals((Integer)1, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.STORES));
      assertEquals(1, remoteCache.clientStatistics().getRemoteStores());
      assertEquals((Integer)1, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.CURRENT_NR_OF_ENTRIES));
      assertEquals(1, remoteCache.size());
      assertFalse(remoteCache.isEmpty());
      assertEquals((Integer)1, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.TOTAL_NR_OF_ENTRIES));
      remoteCache.put("a2","v2");
      assertEquals((Integer)2, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.STORES));
      assertEquals(2, remoteCache.clientStatistics().getRemoteStores());
      assertEquals((Integer)2, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.CURRENT_NR_OF_ENTRIES));
      assertEquals((Integer)2, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.TOTAL_NR_OF_ENTRIES));
      assertEquals(2,remoteCache.size());
      assertFalse(remoteCache.isEmpty());
      remoteCache.put("a2","v3");
      assertEquals((Integer)3, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.STORES));
      assertEquals(3, remoteCache.clientStatistics().getRemoteStores());
      assertEquals((Integer)2, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.CURRENT_NR_OF_ENTRIES));
      assertEquals((Integer)3, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.TOTAL_NR_OF_ENTRIES));
      assertEquals(2, remoteCache.size());
      assertFalse(remoteCache.isEmpty());
   }

   public void testHitsAndMisses() {
      remoteCache.get("a");
      assertEquals((Integer)0, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.HITS));
      assertEquals(0, remoteCache.clientStatistics().getRemoteHits());
      assertEquals((Integer)1, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.MISSES));
      assertEquals(1, remoteCache.clientStatistics().getRemoteMisses());
      remoteCache.put("a","v");
      assertEquals((Integer)0, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.HITS));
      assertEquals(0, remoteCache.clientStatistics().getRemoteHits());
      assertEquals((Integer)1, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.MISSES));
      assertEquals(1, remoteCache.clientStatistics().getRemoteMisses());
      remoteCache.get("a");
      assertEquals((Integer)1, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.HITS));
      assertEquals(1, remoteCache.clientStatistics().getRemoteHits());
      assertEquals((Integer)1, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.MISSES));
      assertEquals(1, remoteCache.clientStatistics().getRemoteMisses());
      remoteCache.get("a");
      remoteCache.get("a");
      remoteCache.get("a");
      assertEquals((Integer)4, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.HITS));
      assertEquals(4, remoteCache.clientStatistics().getRemoteHits());
      assertEquals((Integer)1, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.MISSES));
      assertEquals(1, remoteCache.clientStatistics().getRemoteMisses());
   }

   public void testRemoveHitsAndMisses() {
      remoteCache.remove("a");
      assertEquals((Integer)0, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.REMOVE_HITS));
      assertEquals((Integer)1, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.REMOVE_MISSES));
      remoteCache.put("a","v");
      remoteCache.remove("a");
      assertEquals((Integer)1, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.REMOVE_HITS));
      assertEquals((Integer)1, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.REMOVE_MISSES));
      remoteCache.put("a","v");
      remoteCache.put("b","v");
      remoteCache.put("c","v");

      remoteCache.remove("a");
      remoteCache.remove("b");
      remoteCache.remove("c");
      assertEquals((Integer)4, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.REMOVE_HITS));
      assertEquals((Integer)1, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.REMOVE_MISSES));
   }

   public void testNumberOfEntriesAfterClear() {
      assertEquals((Integer)0, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.CURRENT_NR_OF_ENTRIES));
      remoteCache.put("k", "v");
      remoteCache.put("k2", "v");
      assertEquals((Integer)2, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.CURRENT_NR_OF_ENTRIES));
      remoteCache.clear();
      assertEquals((Integer)0, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.CURRENT_NR_OF_ENTRIES));
      assertEquals((Integer)2, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.TOTAL_NR_OF_ENTRIES));
   }

}
