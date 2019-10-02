package org.infinispan.query.distributed;

import static org.testng.Assert.assertEquals;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.queries.faceting.Car;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * Tests for MassIndexer on sharded indexes
 *
 * @author gustavonalle
 * @since 7.1
 */
@Test(groups = "functional", testName = "query.distributed.ShardingMassIndexTest")
public class ShardingMassIndexTest extends MultipleCacheManagersTest {

   protected static final int NUM_NODES = 3;

   @Override
   @SuppressWarnings("unchecked")
   protected void createCacheManagers() throws Throwable {
      // Car is split into 2 shards: one is going to Infinispan Directory with a shared index, the other is
      // going to Ram with a local index
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cacheCfg
            .indexing()
            .index(Index.ALL)
            .addIndexedEntity(Car.class)
            .addProperty("hibernate.search.car.sharding_strategy.nbr_of_shards", "2")
            .addProperty("hibernate.search.car.1.directory_provider", "local-heap")
            .addProperty("hibernate.search.car.0.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      createClusteredCaches(NUM_NODES, QueryTestSCI.INSTANCE, cacheCfg);
      waitForClusterToForm(getDefaultCacheName());
   }

   public void testReindex() throws Exception {
      Cache<Integer, Object> cache = cache(0);
      cache.put(1, new Car("mazda", "red", 200));
      cache.put(2, new Car("mazda", "blue", 200));
      cache.put(3, new Car("audi", "blue", 170));
      cache.put(4, new Car("audi", "black", 170));

      checkIndex(4, Car.class);

      runMassIndexer();

      checkIndex(4, Car.class);

      cache.clear();
      runMassIndexer();

      checkIndex(0, Car.class);
   }

   protected void checkIndex(int expectedNumber, Class<?> entity) {
      for (Cache<?, ?> c : caches()) {
         SearchManager searchManager = Search.getSearchManager(c);
         assertEquals(searchManager.getQuery(new MatchAllDocsQuery(), entity).getResultSize(), expectedNumber);
      }
   }

   protected void runMassIndexer() throws Exception {
      SearchManager searchManager = Search.getSearchManager(cache(0));
      searchManager.getMassIndexer().start();
   }
}
