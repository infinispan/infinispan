package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.Constants.INTELLIGENCE_HASH_DISTRIBUTION_AWARE;
import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertKeyDoesNotExist;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertSuccess;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;

import java.lang.reflect.Method;
import java.util.Map;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.HotRodMagicKeyGenerator;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.stats.impl.AbstractClusterStats;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.hotrod.HotRodStatsClusterTest")
public class HotRodStatsClusterTest extends HotRodMultiNodeTest {

   @Override
   protected byte protocolVersion() {
      return 24;
   }

   @Override
   protected String cacheName() {
      return "hotRodClusterStats";
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      GlobalConfigurationBuilder global = GlobalConfigurationBuilder.defaultClusteredBuilder();
      global.metrics().accurateSize(true);
      return TestCacheManagerFactory.createClusteredCacheManager(global, hotRodCacheConfiguration());
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      ConfigurationBuilder config = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      config.statistics().enable();
      config.clustering().hash().numOwners(1);
      return config;
   }

   public void testClusterStats(Method m) throws Exception {
      HotRodClient client1 = clients().get(0);
      HotRodClient client2 = clients().get(1);

      byte[] key1 = HotRodMagicKeyGenerator.newKey(cache(0, cacheName()));
      byte[] value = v(m, "v1-");
      TestResponse resp = client1.put(key1, 0, 0, value, INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0);
      assertStatus(resp, Success);
      assertSuccess(client1.get(key1, 0), value);
      client1.remove(k(m));

      Map<String, String> stats1 = client1.stats();
      assertEquals(stats1.get("currentNumberOfEntries"), "1");
      assertEquals(stats1.get("totalNumberOfEntries"), "1");
      assertEquals(stats1.get("stores"), "1");
      assertEquals(stats1.get("hits"), "1");
      assertEquals(stats1.get("retrievals"), "1");
      assertEquals(stats1.get("removeMisses"), "1");
      assertEquals(stats1.get("globalCurrentNumberOfEntries"), "1");
      assertEquals(stats1.get("globalStores"), "1");
      assertEquals(stats1.get("globalHits"), "1");
      assertEquals(stats1.get("globalRetrievals"), "1");
      assertEquals(stats1.get("globalRemoveMisses"), "1");

      Map<String, String> stats2 = client2.stats();
      assertEquals(stats2.get("currentNumberOfEntries"), "0");
      assertEquals(stats2.get("totalNumberOfEntries"), "0");
      assertEquals(stats2.get("stores"), "0");
      assertEquals(stats2.get("hits"), "0");
      assertEquals(stats2.get("retrievals"), "0");
      assertEquals(stats2.get("removeMisses"), "0");
      assertEquals(stats2.get("globalCurrentNumberOfEntries"), "1");
      assertEquals(stats2.get("globalStores"), "1");
      assertEquals(stats2.get("globalHits"), "1");
      assertEquals(stats2.get("globalRetrievals"), "1");
      assertEquals(stats2.get("globalRemoveMisses"), "1");

      TestingUtil.sleepThread(AbstractClusterStats.DEFAULT_STALE_STATS_THRESHOLD + 2000);

      client1.remove(key1);
      assertKeyDoesNotExist(client1.get(key1, 0));

      stats1 = client1.stats();
      assertEquals(stats1.get("misses"), "1");
      assertEquals(stats1.get("removeHits"), "1");
      assertEquals(stats1.get("globalMisses"), "1");
      assertEquals(stats1.get("globalRemoveHits"), "1");

      stats2 = client2.stats();
      assertEquals(stats2.get("misses"), "0");
      assertEquals(stats2.get("removeHits"), "0");
      assertEquals(stats2.get("globalMisses"), "1");
      assertEquals(stats2.get("globalRemoveHits"), "1");
   }

}
