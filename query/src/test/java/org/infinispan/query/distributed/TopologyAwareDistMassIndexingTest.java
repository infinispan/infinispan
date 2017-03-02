package org.infinispan.query.distributed;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.hibernate.search.spi.InfinispanIntegration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.helper.TestQueryHelperFactory;
import org.testng.annotations.Test;

/**
 * Tests verifying that Mass Indexer works properly on Topology Aware nodes.
 */
@Test(groups = "functional", testName = "query.distributed.TopologyAwareDistMassIndexingTest")
public class TopologyAwareDistMassIndexingTest extends DistributedMassIndexingTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      caches = TestQueryHelperFactory.createTopologyAwareCacheNodes(NUM_NODES, CacheMode.DIST_SYNC, false, true, false,
            "default", holder -> {
               Configuration cacheCfg1 = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false)
                     .clustering().stateTransfer().fetchInMemoryState(true).build();
               holder.newConfigurationBuilder(InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME).read(cacheCfg1);
               holder.newConfigurationBuilder(InfinispanIntegration.DEFAULT_LOCKING_CACHENAME).read(cacheCfg1);
            });

      for(Object cache : caches) {
         Cache cacheObj = (Cache) cache;
         cacheManagers.add(cacheObj.getCacheManager());
      }

      waitForClusterToForm(neededCacheNames);
   }
}
