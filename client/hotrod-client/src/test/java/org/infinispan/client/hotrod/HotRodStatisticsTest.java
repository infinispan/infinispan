package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map;

import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(groups = "functional", testName = "client.hotrod.HotRodStatisticsTest")
public class HotRodStatisticsTest extends AbstractInfinispanTest {

   private RemoteCache remoteCache;
   protected HotRodServer[] hotrodServers;

   CacheMode cacheMode;
   private HotRodStatisticsTest cacheMode(CacheMode cacheMode) {
      this.cacheMode = cacheMode;
      return this;
   }
   @Factory
   public Object[] factory() {
      return new Object[]{
            new HotRodStatisticsTest().cacheMode(CacheMode.DIST_SYNC)
      };
   }

   @BeforeMethod
   protected void setup() throws Exception {

      final int numServers = 3;
      hotrodServers = new HotRodServer[numServers];
      for (int i = 0; i < numServers; i++) {
         ConfigurationBuilder cfg = hotRodCacheConfiguration();
         if (CacheMode.DIST_SYNC.equals(cacheMode)) {
            cfg.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numOwners(2);
         } else if (CacheMode.REPL_SYNC.equals(cacheMode)) {
            cfg.clustering().cacheMode(CacheMode.REPL_SYNC);
         } else {
            throw new IllegalStateException("Test scenario not implemented");
         }
         cfg.jmxStatistics().enable();
         String jmxDomain = getClass().getSimpleName();
         boolean exposeGlobalJmx = true;
         boolean allowDuplicateDomains = true;
         hotrodServers[i] = HotRodClientTestingUtil.startHotRodServer(
               TestCacheManagerFactory.createClusteredCacheManagerEnforceJmxDomain(jmxDomain, exposeGlobalJmx, allowDuplicateDomains, cfg));
      }

      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(hotrodServers[0].getPort());
      clientBuilder.clientIntelligence(ClientIntelligence.BASIC);
      clientBuilder.statistics().enable();
      RemoteCacheManager rcm = new RemoteCacheManager(clientBuilder.build());
      remoteCache = rcm.getCache();
   }

   @AfterMethod
   void tearDown() {
      for (HotRodServer server : hotrodServers) {
         TestingUtil.killCacheManagers(server.getCacheManager());
      }
      killRemoteCacheManager(remoteCache.getRemoteCacheManager());
      killServers(hotrodServers);
   }

   public void testAllStatsArePresent() {
      ServerStatistics serverStatistics = remoteCache.serverStatistics();
      Map<String, String> statsMap = serverStatistics.getStatsMap();
      assertEquals("0", statsMap.get(ServerStatistics.STORES));
      assertEquals("0", statsMap.get(ServerStatistics.CURRENT_NR_OF_ENTRIES));
      assertEquals("0", statsMap.get(ServerStatistics.HITS));
      assertEquals("0", statsMap.get(ServerStatistics.MISSES));
      assertEquals("0", statsMap.get(ServerStatistics.REMOVE_HITS));
      assertEquals("0", statsMap.get(ServerStatistics.REMOVE_MISSES));
      assertEquals("0", statsMap.get(ServerStatistics.RETRIEVALS));
      assertEquals("0", statsMap.get(ServerStatistics.TOTAL_NR_OF_ENTRIES));
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

   public void testLocalNonBlockingGetStats() {
      Integer max = 10;
      for (int i = 0; i < max; i++) {
         remoteCache.put("k" + i, "v" + i);

         remoteCache.get("k" + i);
         remoteCache.get("not_presented" + i);
      }
      assertEquals(max, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.HITS));
      assertEquals(max, remoteCache.serverStatistics().getIntStatistic(ServerStatistics.MISSES));
   }

   @Override
   protected String parameters() {
      return "cacheMode-" + cacheMode;
   }

}
