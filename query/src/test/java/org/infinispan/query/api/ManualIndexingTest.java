package org.infinispan.query.api;

import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.api.query.QueryResult;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.queries.faceting.Car;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.api.ManualIndexingTest")
public class ManualIndexingTest extends MultipleCacheManagersTest {

   protected static final int NUM_NODES = 4;
   protected List<Cache<String, Car>> caches = new ArrayList<>(NUM_NODES);

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < NUM_NODES; i++) {
         EmbeddedCacheManager cacheManager = TestCacheManagerFactory.fromXml("manual-indexing-distribution.xml");
         registerCacheManager(cacheManager);
         Cache<String, Car> cache = cacheManager.getCache();
         caches.add(cache);
      }
      waitForClusterToForm("default");
   }

   public void testManualIndexing() {
      caches.get(0).put("car A", new Car("ford", "blue", 400));
      caches.get(0).put("car B", new Car("megane", "white", 300));
      caches.get(0).put("car C", new Car("megane", "red", 500));

      assertNumberOfCars(0, "megane");
      assertNumberOfCars(0, "ford");

      // rebuild index
      join(Search.getIndexer(caches.get(0)).run());

      assertNumberOfCars(2, "megane");
      assertNumberOfCars(1, "ford");
   }

   private void assertNumberOfCars(int expectedCount, String carMake) {
      for (Cache<?, ?> cache : caches) {
         Query<Car> query = cache.query(String.format("FROM %s where make:'%s'", Car.class.getName(), carMake));
         QueryResult<Car> queryResult = query.execute();
         assertEquals("Expected count not met on cache " + cache, expectedCount, queryResult.count().value());
         assertEquals("Expected count not met on cache " + cache, expectedCount, queryResult.list().size());
      }
   }
}
