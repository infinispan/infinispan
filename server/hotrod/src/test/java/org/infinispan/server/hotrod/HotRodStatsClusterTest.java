package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.Constants.INTELLIGENCE_HASH_DISTRIBUTION_AWARE;
import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertKeyDoesNotExist;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertSuccess;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Method;
import java.util.Map;

import org.infinispan.commands.module.TestGlobalConfigurationBuilder;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.HotRodMagicKeyGenerator;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.stats.impl.AbstractClusterStats;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.hotrod.HotRodStatsClusterTest")
public class HotRodStatsClusterTest extends HotRodMultiNodeTest {
   ControlledTimeService timeService = new ControlledTimeService();

   @Override
   protected byte protocolVersion() {
      return 24;
   }

   @Override
   protected String cacheName() {
      return "hotRodClusterStats";
   }

   @Override
   protected void createAndAddCacheManager() {
      GlobalConfigurationBuilder global = defaultGlobalConfigurationBuilder();
      global.metrics().accurateSize(true);
      global.addModule(TestGlobalConfigurationBuilder.class)
            .testGlobalComponent(TimeService.class, timeService);
      //noinspection resource
      addClusterEnabledCacheManager(global, hotRodCacheConfiguration());
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
      assertEquals("1", stats1.get("currentNumberOfEntries"));
      assertEquals("1", stats1.get("stores"));
      assertEquals("1", stats1.get("hits"));
      assertEquals("1", stats1.get("retrievals"));
      assertEquals("1", stats1.get("removeMisses"));
      assertEquals("1", stats1.get("globalCurrentNumberOfEntries"));
      assertEquals("1", stats1.get("globalStores"));
      assertEquals("1", stats1.get("globalHits"));
      assertEquals("1", stats1.get("globalRetrievals"));
      assertEquals("1", stats1.get("globalRemoveMisses"));

      Map<String, String> stats2 = client2.stats();
      assertEquals("0", stats2.get("currentNumberOfEntries"));
      assertEquals("0", stats2.get("stores"));
      assertEquals("0", stats2.get("hits"));
      assertEquals("0", stats2.get("retrievals"));
      assertEquals("0", stats2.get("removeMisses"));
      assertEquals("1", stats2.get("globalCurrentNumberOfEntries"));
      assertEquals("1", stats2.get("globalStores"));
      assertEquals("1", stats2.get("globalHits"));
      assertEquals("1", stats2.get("globalRetrievals"));
      assertEquals("1", stats2.get("globalRemoveMisses"));

      timeService.advance(AbstractClusterStats.DEFAULT_STALE_STATS_THRESHOLD + 1);

      client1.remove(key1);
      assertKeyDoesNotExist(client1.get(key1, 0));

      stats1 = client1.stats();
      assertEquals("1", stats1.get("misses"));
      assertEquals("1", stats1.get("removeHits"));
      assertEquals("1", stats1.get("globalMisses"));
      assertEquals("1", stats1.get("globalRemoveHits"));

      stats2 = client2.stats();
      assertEquals("0", stats2.get("misses"));
      assertEquals("0", stats2.get("removeHits"));
      assertEquals("1", stats2.get("globalMisses"));
      assertEquals("1", stats2.get("globalRemoveHits"));
   }

}
