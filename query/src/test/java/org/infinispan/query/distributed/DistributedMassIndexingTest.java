package org.infinispan.query.distributed;

import static org.infinispan.functional.FunctionalTestUtils.await;
import static org.testng.Assert.assertEquals;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ClusteringConfiguration;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Search;
import org.infinispan.query.core.stats.IndexInfo;
import org.infinispan.query.core.stats.IndexStatisticsSnapshot;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.queries.faceting.Car;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author Sanne Grinovero &lt;sanne@hibernate.org&gt; (C) 2012 Red Hat Inc.
 */
@Test(groups = "functional", testName = "query.distributed.DistributedMassIndexingTest")
public class DistributedMassIndexingTest extends MultipleCacheManagersTest {

   protected static final int NUM_NODES = 3;

   {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected String getConfigurationFile() {
      return "dynamic-indexing-distribution.xml";
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      for (int i = 0; i < NUM_NODES; i++) {
         EmbeddedCacheManager cacheManager = TestCacheManagerFactory.fromXml(getConfigurationFile());
         registerCacheManager(cacheManager);
         cacheManager.getCache();
      }
      waitForClusterToForm();
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      super.clearContent();
   }

   public void testReindexing() throws Exception {
      cache(0).put(key("F1NUM"), new Car("megane", "white", 300));
      verifyFindsCar(1, "megane");
      cache(1).put(key("F2NUM"), new Car("megane", "blue", 300));
      verifyFindsCar(2, "megane");
      //add an entry without indexing it:
      cache(1).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).put(key("F3NUM"), new Car("megane", "blue", 300));
      verifyFindsCar(2, "megane");
      //re-sync datacontainer with indexes:
      rebuildIndexes();
      verifyFindsCar(3, "megane");
      //verify we cleanup old stale index values by deleting an entry without deleting the corresponding index value
      cache(2).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).remove(key("F2NUM"));
      assertIndexedEntities(3, Car.class, cache(2));
      //re-sync
      rebuildIndexes();
      assertIndexedEntities(2, Car.class, cache(2));
      verifyFindsCar(2, "megane");
   }

   public void testPartiallyReindex() throws Exception {
      cache(0).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).put(key("F1NUM"), new Car("megane", "white", 300));
      Search.getIndexer(cache(0)).run(key("F1NUM")).toCompletableFuture().join();
      verifyFindsCar(1, "megane");
      cache(0).remove(key("F1NUM"));
      verifyFindsCar(0, "megane");
   }

   protected Object key(String keyId) {
      //Used to verify remoting is fine with non serializable keys
      return new NonSerializableKeyType(keyId);
   }

   protected void rebuildIndexes() throws Exception {
      Cache<?, ?> cache = cache(0);
      CompletionStages.join(Search.getIndexer(cache).run());
   }

   protected void verifyFindsCar(int expectedCount, String carMake) {
      for (Cache<String, Car> cache : this.<String, Car>caches()) {
         StaticTestingErrorHandler.assertAllGood(cache);
         verifyFindsCar(cache, expectedCount, carMake);
      }
   }

   protected void verifyFindsCar(Cache<?, Car> cache, int expectedCount, String carMake) {
      String q = String.format("FROM %s where make:'%s'", Car.class.getName(), carMake);
      Query<Car> cacheQuery = Search.getQueryFactory(cache).create(q);
      assertEquals(cacheQuery.list().size(), expectedCount);
   }

   private void assertIndexedEntities(int expected, Class<?> entityClass, Cache<?, Car> cache) {
      IndexStatisticsSnapshot indexStatistics = await(Search.getClusteredSearchStatistics(cache)).getIndexStatistics();
      IndexInfo indexInfo = indexStatistics.indexInfos().get(entityClass.getName());
      int count = (int) indexInfo.count();
      // each entry is indexed in all owners for redundancy
      final ClusteringConfiguration clusteringConfiguration = cache.getCacheConfiguration().clustering();
      long indexedCount;
      if (clusteringConfiguration.cacheMode().isReplicated()) {
         indexedCount = count / caches().size();
      } else {
         indexedCount = count / clusteringConfiguration.hash().numOwners();
      }
      assertEquals(indexedCount, expected);
   }
}
