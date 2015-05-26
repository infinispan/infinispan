package org.infinispan.query.distributed;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.infinispan.Cache;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.queries.faceting.Car;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;

/**
 * Tests for MassIndexer on sharded indexes
 *
 * @author gustavonalle
 * @since 7.1
 */
@Test(groups = "functional", testName = "query.distributed.ShardingMassIndexTest")
public class ShardingMassIndexTest extends MultipleCacheManagersTest {

   protected static final int NUM_NODES = 3;
   protected List<Cache<Integer, Object>> caches = new ArrayList<>(NUM_NODES);

   @Override
   @SuppressWarnings("unchecked")
   protected void createCacheManagers() throws Throwable {
      // Car is split into 2 shards: one is going to Infinispan Directory with a shared index, the other is
      // going to Ram with a local index
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      cacheCfg
            .indexing()
            .index(Index.ALL)
            .addProperty("hibernate.search.car.sharding_strategy.nbr_of_shards", "2")
            .addProperty("hibernate.search.car.1.directory_provider", "ram")
            .addProperty("hibernate.search.car.0.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler");

      List<Cache<Integer, Object>> cacheList = createClusteredCaches(NUM_NODES, cacheCfg);

      waitForClusterToForm(BasicCacheContainer.DEFAULT_CACHE_NAME);

      for (Cache cache : cacheList) {
         caches.add(cache);
      }
   }

   public void testReindex() throws Exception {
      caches.get(0).put(1, new Car("mazda", "red", 200));
      caches.get(0).put(2, new Car("mazda", "blue", 200));
      caches.get(0).put(3, new Car("audi", "blue", 170));
      caches.get(0).put(4, new Car("audi", "black", 170));

      checkIndex(4, Car.class);

      runMassIndexer();

      checkIndex(4, Car.class);

      caches.get(0).clear();
      runMassIndexer();

      checkIndex(0, Car.class);
   }

   protected void checkIndex(int expectedNumber, Class<?> entity) {
      for (Cache<?, ?> c : caches) {
         SearchManager searchManager = Search.getSearchManager(c);
         assertEquals(searchManager.getQuery(new MatchAllDocsQuery(), entity).getResultSize(), expectedNumber);
      }
   }

   protected void runMassIndexer() throws Exception {
      Cache cache = caches.get(0);
      SearchManager searchManager = Search.getSearchManager(cache);
      searchManager.getMassIndexer().start();
   }
}
