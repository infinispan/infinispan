package org.infinispan.query.distributed;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.Indexer;
import org.infinispan.query.Search;
import org.infinispan.query.queries.faceting.Car;
import org.infinispan.query.test.QueryTestSCI;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional", testName = "query.distributed.ReplRamMassIndexingTest")
public class ReplRamMassIndexingTest extends DistributedMassIndexingTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cacheCfg
            .indexing()
            .enable()
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Car.class)
            .clustering()
            .hash().numSegments(10 * NUM_NODES);
      cacheCfg.clustering().stateTransfer().fetchInMemoryState(true);
      createClusteredCaches(NUM_NODES, QueryTestSCI.INSTANCE, cacheCfg);

      waitForClusterToForm(getDefaultCacheName());
   }

   @Override
   public void testReindexing() throws Exception {
      final int NUM_CARS = 100;
      for (int i = 0; i < NUM_CARS; ++i) {
         cache(i % NUM_NODES).put("car" + i, new Car("skoda", "white", 42));
      }
      for (Cache cache : caches()) {
         assertEquals(cache.size(), NUM_CARS);
         verifyFindsCar(cache, NUM_CARS, "skoda");
      }
      rebuildIndexes();
      for (int i = 0; i < NUM_CARS; ++i) {
         String key = "car" + i;
         for (Cache cache : caches()) {
            assertNotNull(cache.get(key));
         }
      }
      verifyFindsCar(NUM_CARS, "skoda");
   }

   @Override
   protected void rebuildIndexes() {
      for (Cache<?, ?> cache : caches()) {
         Indexer indexer = Search.getIndexer(cache);
         eventually(() -> !indexer.isRunning());
         join(indexer.run());
      }
   }
}
