package org.infinispan.query.distributed;

import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.search.Query;
import org.hibernate.search.query.dsl.QueryBuilder;
import org.infinispan.Cache;
import org.infinispan.commons.test.ThreadLeakChecker;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.queries.faceting.Car;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2012 Red Hat Inc.
 */
@Test(groups = "functional", testName = "query.distributed.DistributedMassIndexingTest")
public class DistributedMassIndexingTest extends MultipleCacheManagersTest {

   protected static final int NUM_NODES = 3;
   protected static final String[] neededCacheNames = new String[]{
         "default",
         "LuceneIndexesMetadata",
         "LuceneIndexesData",
         "LuceneIndexesLocking",
   };

   protected List<Cache> caches = new ArrayList<>(NUM_NODES);

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

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      // DistributedExecutorMassIndexer leaks executor, see ISPN-7606
      ThreadLeakChecker.ignoreThreadsContaining("DefaultExecutorService-");

      super.destroy();
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

   public void testPartiallyReindex() throws Exception {
      caches.get(0).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).put(key("F1NUM"), new Car("megane", "white", 300));
      Search.getSearchManager(cache(0)).getMassIndexer().reindex(key("F1NUM")).get();
      verifyFindsCar(1, "megane");
      caches.get(0).remove(key("F1NUM"));
      verifyFindsCar(0, "megane");
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
      for (Cache cache : caches) {
         StaticTestingErrorHandler.assertAllGood(cache);
         verifyFindsCar(cache, expectedCount, carMake);
      }
   }

   protected void verifyFindsCar(Cache cache, int expectedCount, String carMake) throws Exception {
      SearchManager searchManager = Search.getSearchManager(cache);
      QueryBuilder carQueryBuilder = searchManager.buildQueryBuilderForClass(Car.class).get();
      Query fullTextQuery = carQueryBuilder.keyword().onField("make").matching(carMake).createQuery();
      CacheQuery<Car> cacheQuery = searchManager.getQuery(fullTextQuery, Car.class);
      assertEquals(expectedCount, cacheQuery.getResultSize());
   }

   @Override
   protected boolean cleanupAfterTest() {
      return false;
   }

   @Override
   protected boolean cleanupAfterMethod() {
      return true;
   }

   @BeforeMethod
   @Override
   public void createBeforeMethod() throws Throwable {
      caches.clear();
      super.createBeforeMethod();
   }
}
