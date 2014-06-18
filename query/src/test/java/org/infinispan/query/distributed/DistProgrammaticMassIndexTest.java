package org.infinispan.query.distributed;

import junit.framework.Assert;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.hibernate.search.infinispan.InfinispanIntegration;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.queries.faceting.Car;
import org.testng.annotations.Test;

import java.util.List;

import static org.infinispan.query.helper.TestQueryHelperFactory.createQueryParser;

/**
 * Tests verifying that the Mass Indexing for programmatic cache configuration works as well.
 *
 * @author Anna Manukyan
 */
@Test(groups = /*functional*/"unstable", testName = "query.distributed.DistProgrammaticMassIndexTest", description = "Unstable, see https://issues.jboss.org/browse/ISPN-4012")
public class DistProgrammaticMassIndexTest extends DistributedMassIndexingTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cacheCfg.indexing()
            .enable()
            .indexLocalOnly(true)
            .addProperty("hibernate.search.default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("hibernate.search.default.directory_provider", "infinispan")
            .addProperty("hibernate.search.default.exclusive_index_use", "false")
            .addProperty("lucene_version", "LUCENE_48");
      cacheCfg.clustering().stateTransfer().fetchInMemoryState(true);
      List<Cache<String, Car>> cacheList = createClusteredCaches(NUM_NODES, cacheCfg);

      for(int i = 0; i < NUM_NODES; i++) {
         ConfigurationBuilder cacheCfg1 = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
         cacheCfg1.clustering().stateTransfer().fetchInMemoryState(true);
         cacheManagers.get(i).defineConfiguration(InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME, cacheCfg1.build());
         cacheManagers.get(i).defineConfiguration( InfinispanIntegration.DEFAULT_LOCKING_CACHENAME, cacheCfg1.build());
      }

      waitForClusterToForm(neededCacheNames);

      for(Cache cache : cacheList) {
         caches.add(cache);
      }
   }

   @Test(groups = "unstable")
   @Override
   public void testReindexing() throws Exception {
      super.testReindexing();
   }

   protected void verifyFindsCar(Cache cache, int count, String carMake) {
      QueryParser queryParser = createQueryParser("make");

      try {
         Query luceneQuery = queryParser.parse(carMake);
         CacheQuery cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery, Car.class);

         Assert.assertEquals(count, cacheQuery.getResultSize());

      } catch(ParseException ex) {
         ex.printStackTrace();
         Assert.fail("Failed due to: " + ex.getMessage());
      }
   }

}
