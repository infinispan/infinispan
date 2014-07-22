package org.infinispan.query.distributed;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.Query;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.queries.faceting.Car;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2012 Red Hat Inc.
 */
@Test(groups = "functional", testName = "query.distributed.DistributedMassIndexingTest")
public class DistributedMassIndexingTest extends MultipleCacheManagersTest {

   protected static final int NUM_NODES = 3;
   protected List<Cache> caches = new ArrayList<>(NUM_NODES);

   protected static final String[] neededCacheNames = new String[] {
      BasicCacheContainer.DEFAULT_CACHE_NAME,
      "LuceneIndexesMetadata",
      "LuceneIndexesData",
      "LuceneIndexesLocking",
   };

   protected String getConfigurationFile() {
      return "dynamic-indexing-distribution.xml";
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < NUM_NODES; i++) {
         EmbeddedCacheManager cacheManager = TestCacheManagerFactory.fromXml(getConfigurationFile());
         registerCacheManager(cacheManager);
         Cache cache = cacheManager.getCache();
         caches.add(cache);
      }
      waitForClusterToForm(neededCacheNames);
   }

   public void testReindexing() throws Exception {
      caches.get(0).put(key("F1NUM"), new Car("megane", "white", 300));
      verifyFindsCar(1, "megane");
      caches.get(1).put(key("F2NUM"), new Car("megane", "blue", 300));
      verifyFindsCar(2, "megane");
      //add an entry without indexing it:
      caches.get(1).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).put(key("F3NUM"), new Car("megane", "blue", 300));
      verifyFindsCar(2, "megane");
      //re-sync datacontainer with indexes:
      rebuildIndexes();
      verifyFindsCar(3, "megane");
      //verify we cleanup old stale index values:
      caches.get(2).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).remove(key("F2NUM"));
      verifyFindsCar(3, "megane");
      //re-sync
      rebuildIndexes();
      verifyFindsCar(2, "megane");
   }

   protected Object key(String keyId) {
      //Used to verify remoting is fine with non serializable keys
      return new NonSerializableKeyType(keyId);
   }

   protected void rebuildIndexes() throws Exception {
      Cache cache = caches.get(0);
      SearchManager searchManager = Search.getSearchManager(cache);
      searchManager.getMassIndexer().start();
   }

   protected void verifyFindsCar(int expectedCount, String carMake) throws Exception {
      for (Cache cache: caches) {
         StaticTestingErrorHandler.assertAllGood(cache);
         verifyFindsCar(cache, expectedCount, carMake);
      }
   }

   protected void verifyFindsCar(Cache cache, int expectedCount, String carMake) throws Exception {
      SearchManager searchManager = Search.getSearchManager(cache);
      QueryBuilder carQueryBuilder = searchManager.buildQueryBuilderForClass(Car.class).get();
      Query fullTextQuery = carQueryBuilder.keyword().onField("make").matching(carMake).createQuery();
      CacheQuery cacheQuery = searchManager.getQuery(fullTextQuery, Car.class);
      assertEquals(expectedCount, cacheQuery.getResultSize());
   }
}
