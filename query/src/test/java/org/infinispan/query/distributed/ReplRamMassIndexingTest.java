package org.infinispan.query.distributed;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.Search;
import org.infinispan.query.queries.faceting.Car;
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
            .index(Index.ALL)
            .addIndexedEntity(Car.class)
            .addProperty("hibernate.search.default.directory_provider", "ram")
            .addProperty("hibernate.search.default.exclusive_index_use", "true")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT")
            .clustering()
            .hash().numSegments(10 * NUM_NODES);
      cacheCfg.clustering().stateTransfer().fetchInMemoryState(true);
      List<Cache<String, Car>> cacheList = createClusteredCaches(NUM_NODES, cacheCfg);

      waitForClusterToForm(BasicCacheContainer.DEFAULT_CACHE_NAME);

      for(Cache cache : cacheList) {
         caches.add(cache);
      }
   }

   @Override
   public void testReindexing() throws Exception {
      final int NUM_CARS = 100;
      for (int i = 0; i < NUM_CARS; ++i) {
         caches.get(i % NUM_NODES).put("car" + i, new Car("skoda", "white", 42));
      }
      for (Cache cache : caches) {
         assertEquals(cache.size(), NUM_CARS);
         verifyFindsCar(cache, NUM_CARS, "skoda");
      }
      rebuildIndexes();
      for (int i = 0; i < NUM_CARS; ++i) {
         String key = "car" + i;
         for (Cache cache : caches) {
            assertNotNull(cache.get(key));
         }
      }
      verifyFindsCar(NUM_CARS, "skoda");
   }

   @Override
   protected void rebuildIndexes() throws Exception {
      for (Cache cache : caches) {
         Search.getSearchManager(cache).getMassIndexer().start();
      }
   }
}
