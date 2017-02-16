package org.infinispan.query.distributed;

import static org.infinispan.query.helper.TestQueryHelperFactory.createQueryParser;
import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.hibernate.search.spi.InfinispanIntegration;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.indexmanager.InfinispanIndexManager;
import org.infinispan.query.queries.faceting.Car;
import org.testng.annotations.Test;

/**
 * Tests verifying that the Mass Indexing for programmatic cache configuration works as well.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.distributed.DistProgrammaticMassIndexTest")
public class DistProgrammaticMassIndexTest extends DistributedMassIndexingTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cacheCfg.indexing()
            .index(Index.LOCAL)
            .addIndexedEntity(Car.class)
            .addProperty("default.indexmanager", InfinispanIndexManager.class.getName())
            .addProperty("error_handler", StaticTestingErrorHandler.class.getName())
            .addProperty("lucene_version", "LUCENE_CURRENT");
      cacheCfg.clustering().stateTransfer().fetchInMemoryState(true);
      List<Cache<String, Car>> cacheList = createClusteredCaches(NUM_NODES, "default", cacheCfg);

      for(int i = 0; i < NUM_NODES; i++) {
         ConfigurationBuilder cacheCfg1 = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
         cacheCfg1.clustering().stateTransfer().fetchInMemoryState(true);
         cacheManagers.get(i).defineConfiguration(InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME, cacheCfg1.build());
         cacheManagers.get(i).defineConfiguration(InfinispanIntegration.DEFAULT_LOCKING_CACHENAME, cacheCfg1.build());
      }

      waitForClusterToForm(neededCacheNames);

      for(Cache cache : cacheList) {
         caches.add(cache);
      }
   }

   protected void verifyFindsCar(Cache cache, int count, String carMake) throws Exception {
      QueryParser queryParser = createQueryParser("make");

      Query luceneQuery = queryParser.parse(carMake);
      CacheQuery<?> cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery, Car.class);

      assertEquals(count, cacheQuery.getResultSize());

      StaticTestingErrorHandler.assertAllGood(cache);
   }

}
