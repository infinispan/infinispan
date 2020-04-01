package org.infinispan.query.distributed;

import static org.infinispan.query.helper.TestQueryHelperFactory.createTopologyAwareCacheNodes;

import java.util.List;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.queries.faceting.Car;
import org.testng.annotations.Test;

/**
 * Tests verifying that Mass Indexer works properly on Topology Aware nodes.
 */
@Test(groups = "functional", testName = "query.distributed.TopologyAwareDistMassIndexingTest")
public class TopologyAwareDistMassIndexingTest extends DistributedMassIndexingTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      List<EmbeddedCacheManager> managers =
            createTopologyAwareCacheNodes(NUM_NODES, CacheMode.DIST_SYNC, false, true,
                  false, getClass().getSimpleName(), Car.class);

      registerCacheManager(managers.toArray(new CacheContainer[0]));

      waitForClusterToForm();
   }
}
