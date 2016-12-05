package org.infinispan.counter.impl;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.test.MultipleCacheManagersTest;

/**
 * @author Pedro Ruivo
 * @since 9.0
 */
public abstract class BaseCounterTest extends MultipleCacheManagersTest {

   protected abstract int clusterSize();

   protected GlobalConfigurationBuilder configure(int nodeId) {
      return GlobalConfigurationBuilder.defaultClusteredBuilder();
   }

   @Override
   protected final void createCacheManagers() throws Throwable {
      final int size = clusterSize();
      for (int i = 0; i < size; ++i) {
         addClusterEnabledCacheManager(configure(i), getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
      }
      waitForCounterCaches();
   }

   protected final void waitForCounterCaches() {
      waitForClusterToForm(null, CounterModuleLifecycle.COUNTER_CONFIGURATION_CACHE_NAME,
            CounterModuleLifecycle.COUNTER_CACHE_NAME);
   }

   protected final CounterManager counterManager(int index) {
      return EmbeddedCounterManagerFactory.asCounterManager(manager(index));
   }
}
