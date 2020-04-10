package org.infinispan.metrics;

import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

import java.util.SortedMap;

import org.eclipse.microprofile.metrics.Gauge;
import org.eclipse.microprofile.metrics.MetricID;
import org.eclipse.microprofile.metrics.Tag;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metrics.impl.MetricsCollector;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TransportFlags;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 10.1
 */
@Test(groups = "functional", testName = "metrics.ClusteredCacheManagerMetricsTest")
public class ClusteredCacheManagerMetricsTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = "MyCache";

   @Override
   protected void createCacheManagers() throws Throwable {
      GlobalConfigurationBuilder globalConfig1 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfig1.cacheContainer().statistics(true)
                   .metrics().prefix("ispn").gauges(true).histograms(true).namesAsTags(true);

      ConfigurationBuilder config = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC);
      config.statistics().enable();

      EmbeddedCacheManager cacheManager1 = createClusteredCacheManager(globalConfig1, config, new TransportFlags());
      cacheManager1.start();

      GlobalConfigurationBuilder globalConfig2 = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalConfig2.metrics().prefix("ispn").gauges(true).histograms(true).namesAsTags(true);

      EmbeddedCacheManager cacheManager2 = createClusteredCacheManager(globalConfig2, config, new TransportFlags());
      cacheManager2.start();

      registerCacheManager(cacheManager1, cacheManager2);

      defineConfigurationOnAllManagers(CACHE_NAME, config);

      manager(0).getCache(CACHE_NAME);
      manager(1).getCache(CACHE_NAME);
   }

   public void testMetricsAreRegistered() {
      MetricsCollector mc0 = manager(0).getGlobalComponentRegistry().getComponent(MetricsCollector.class);
      SortedMap<MetricID, Gauge> gauges0 = mc0.getRegistry().getGauges((metricID, metric) -> metricID.getName().contains("ispn"));
      assertFalse(gauges0.isEmpty());

      MetricsCollector mc1 = manager(1).getGlobalComponentRegistry().getComponent(MetricsCollector.class);
      SortedMap<MetricID, Gauge> gauges1 = mc1.getRegistry().getGauges((metricID, metric) -> metricID.getName().startsWith("ispn"));
      assertFalse(gauges1.isEmpty());

      GlobalConfiguration gcfg0 = manager(0).getCacheManagerConfiguration();
      Tag nodeNameTag = new Tag(Constants.NODE_TAG_NAME, gcfg0.transport().nodeName());
      Tag cacheManagerTag = new Tag(Constants.CACHE_MANAGER_TAG_NAME, gcfg0.cacheManagerName());
      MetricID evictionsMetricId = new MetricID("ispn_cache_container_stats_evictions", nodeNameTag, cacheManagerTag);

      Gauge<?> evictions = mc0.getRegistry().getGauges().get(evictionsMetricId);
      assertNotNull(evictions);
   }
}
