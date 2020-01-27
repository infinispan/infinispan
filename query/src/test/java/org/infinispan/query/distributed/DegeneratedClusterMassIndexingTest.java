package org.infinispan.query.distributed;

import static org.infinispan.query.dsl.IndexedQueryMode.FETCH;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.queries.faceting.Car;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 6.1
 */
@Test(groups = "functional", testName = "query.distributed.DegeneratedClusterMassIndexingTest")
public class DegeneratedClusterMassIndexingTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      cfg.indexing()
            .enable()
            .addIndexedEntity(Car.class)
            .addProperty("default.directory_provider", "local-heap")
            .addProperty("lucene_version", "LUCENE_CURRENT");

      addClusterEnabledCacheManager(cfg);
      waitForClusterToForm();
   }

   public void testReindexing() {
      Cache cache = cache(0).getAdvancedCache().withFlags(Flag.SKIP_INDEXING);

      SearchManager searchManager = Search.getSearchManager(cache);

      cache.put("car1", new Car("ford", "white", 300));
      cache.put("car2", new Car("ford", "blue", 300));
      cache.put("car3", new Car("ford", "red", 300));

      // ensure these were not indexed
      String q = String.format("FROM %s where make:'ford'", Car.class.getName());
      CacheQuery<Object> query = searchManager.getQuery(q, FETCH);
      assertEquals(0, query.getResultSize());

      //reindex
      searchManager.getMassIndexer().start();

      // check that the indexing is complete immediately
      assertEquals(3, searchManager.getQuery(q, FETCH).getResultSize());
   }
}
