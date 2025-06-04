package org.infinispan.test.hibernate.cache.commons.util;

import static org.infinispan.hibernate.cache.spi.InfinispanProperties.DEF_PENDING_PUTS_RESOURCE;
import static org.infinispan.hibernate.cache.spi.InfinispanProperties.DEF_TIMESTAMPS_RESOURCE;

import java.util.Map;
import java.util.Properties;

import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.TransportConfigurationBuilder;
import org.infinispan.configuration.parsing.ConfigurationBuilderHolder;
import org.infinispan.transaction.TransactionMode;

public class TestConfigurationHook {

   private final boolean transactional;
   private final CacheMode cacheMode;
   private final boolean pendingPutsSimple;
   private final boolean stats;

   public TestConfigurationHook(Properties properties) {
      transactional = (boolean) properties.getOrDefault(TestRegionFactory.TRANSACTIONAL, false);
      cacheMode = (CacheMode) properties.getOrDefault(TestRegionFactory.CACHE_MODE, null);
      pendingPutsSimple = (boolean) properties.getOrDefault(TestRegionFactory.PENDING_PUTS_SIMPLE, true);
      stats = (boolean) properties.getOrDefault(TestRegionFactory.STATS, false);
   }

   public void amendConfiguration(ConfigurationBuilderHolder holder) {
      TransportConfigurationBuilder transport = holder.getGlobalConfigurationBuilder().transport();
      transport.nodeName(TestResourceTracker.getNextNodeName());
      transport.clusterName(TestResourceTracker.getCurrentTestName());
      for (Map.Entry<String, ConfigurationBuilder> cfg : holder.getNamedConfigurationBuilders().entrySet()) {
         amendCacheConfiguration(cfg.getKey(), cfg.getValue());
      }
      // disable simple cache for testing as we need to insert interceptors
      if (!pendingPutsSimple) {
         holder.getNamedConfigurationBuilders().get(DEF_PENDING_PUTS_RESOURCE).simpleCache(false);
      }
   }

   public void amendCacheConfiguration(String cacheName, ConfigurationBuilder configurationBuilder) {
      if (cacheName.equals(DEF_PENDING_PUTS_RESOURCE)) {
         return;
      }
      if (transactional) {
         if (!cacheName.endsWith("query") && !cacheName.equals(DEF_TIMESTAMPS_RESOURCE) && !cacheName.endsWith(DEF_PENDING_PUTS_RESOURCE)) {
            configurationBuilder.transaction().transactionMode(TransactionMode.TRANSACTIONAL).useSynchronization(true);
         }
      } else {
         configurationBuilder.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      }
      if (cacheMode != null) {
         if (configurationBuilder.clustering().cacheMode().isInvalidation()) {
            configurationBuilder.clustering().cacheMode(cacheMode);
         }
      }
      if (stats) {
         configurationBuilder.statistics().enable();
      }
   }
}
