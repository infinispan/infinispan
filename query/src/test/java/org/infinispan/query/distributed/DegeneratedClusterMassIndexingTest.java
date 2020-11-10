package org.infinispan.query.distributed;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
import org.infinispan.query.dsl.QueryFactory;
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
            .storage(LOCAL_HEAP)
            .addIndexedEntity(Car.class);

      addClusterEnabledCacheManager(cfg);
      waitForClusterToForm();
   }

   public void testReindexing() {
      Cache<String, Car> cache = this.<String, Car>cache(0).getAdvancedCache().withFlags(Flag.SKIP_INDEXING);
      cache.put("car1", new Car("ford", "white", 300));
      cache.put("car2", new Car("ford", "blue", 300));
      cache.put("car3", new Car("ford", "red", 300));

      QueryFactory queryFactory = Search.getQueryFactory(cache);

      // ensure these were not indexed
      String q = String.format("FROM %s where make:'ford'", Car.class.getName());
      Query<Car> query = queryFactory.create(q);
      assertEquals(0, query.execute().hitCount().orElse(-1));

      //reindex
      join(Search.getIndexer(cache).run());

      // check that the indexing is complete immediately
      assertEquals(3, query.execute().hitCount().orElse(-1));
   }
}
