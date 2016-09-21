package org.infinispan.query.api;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.Query;
import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.queries.faceting.Car;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "query.api.ManualIndexingTest")
public class ManualIndexingTest extends MultipleCacheManagersTest {

   protected static final int NUM_NODES = 4;
   protected List<Cache<String, Car>> caches = new ArrayList<>(NUM_NODES);

   protected static final String[] neededCacheNames = {
         BasicCacheContainer.DEFAULT_CACHE_NAME,
         "LuceneIndexesMetadata",
         "LuceneIndexesData",
         "LuceneIndexesLocking",
   };

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < NUM_NODES; i++) {
         EmbeddedCacheManager cacheManager = TestCacheManagerFactory.fromXml("manual-indexing-distribution.xml");
         registerCacheManager(cacheManager);
         Cache<String, Car> cache = cacheManager.getCache();
         caches.add(cache);
      }
      waitForClusterToForm(neededCacheNames);
   }

   public void testManualIndexing() throws Exception {
      caches.get(0).put("car A", new Car("ford", "blue", 400));
      caches.get(0).put("car B", new Car("megane", "white", 300));
      caches.get(0).put("car C", new Car("megane", "red", 500));

      assertNumberOfCars(0, "megane");
      assertNumberOfCars(0, "ford");

      // rebuild index
      Search.getSearchManager(caches.get(0)).getMassIndexer().start();

      assertNumberOfCars(2, "megane");
      assertNumberOfCars(1, "ford");
   }

   private void assertNumberOfCars(int expectedCount, String carMake) {
      for (Cache cache : caches) {
         SearchManager sm = Search.getSearchManager(cache);
         Query query = sm.buildQueryBuilderForClass(Car.class).get().keyword().onField("make").matching(carMake).createQuery();
         CacheQuery<Car> cacheQuery = sm.getQuery(query, Car.class);
         assertEquals("Expected count not met on cache " + cache, expectedCount, cacheQuery.getResultSize());
         assertEquals("Expected count not met on cache " + cache, expectedCount, cacheQuery.list().size());
      }
   }
}
