package org.infinispan.stats;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "stats.NotAvailableSingleStatsTest")
public class NotAvailableSingleStatsTest extends MultipleCacheManagersTest {
   protected Cache cache;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      cfg.statistics().available(false);
      addClusterEnabledCacheManager(cfg);
      cache = cache(0);
   }

   public void testStatsWhenNotAvailable() {
      Stats stats = cache.getAdvancedCache().getStats();
      assertNotNull(stats);
      assertEquals(-1, stats.getHits());
   }
}
