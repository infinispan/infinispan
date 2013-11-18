package org.infinispan.query.distributed;

import org.hibernate.search.infinispan.InfinispanIntegration;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
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
      caches = TestQueryHelperFactory.createTopologyAwareCacheNodes(NUM_NODES, CacheMode.DIST_SYNC, false, true, false);

      for(Object cache : caches) {
         Cache cacheObj = (Cache) cache;
         EmbeddedCacheManager cm1 = cacheObj.getCacheManager();
         ConfigurationBuilder cacheCfg1 = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
         cacheCfg1.clustering().stateTransfer().fetchInMemoryState(true);
         cm1.defineConfiguration(InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME, cacheCfg1.build());
         cm1.defineConfiguration(InfinispanIntegration.DEFAULT_LOCKING_CACHENAME, cacheCfg1.build());

         cacheManagers.add(cm1);
      }

      waitForClusterToForm(neededCacheNames);
   }
}
