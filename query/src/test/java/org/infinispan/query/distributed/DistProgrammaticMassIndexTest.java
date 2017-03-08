package org.infinispan.query.distributed;

import static org.infinispan.query.helper.TestQueryHelperFactory.createQueryParser;
import static org.testng.AssertJUnit.assertEquals;

import java.util.List;

import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
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
      createCluster(holder -> {
         String defaultName = "default";
         holder.getGlobalConfigurationBuilder().defaultCacheName(defaultName);

         ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
         cacheCfg.indexing()
               .index(Index.LOCAL)
               .addIndexedEntity(Car.class)
               .addProperty("default.indexmanager", InfinispanIndexManager.class.getName())
               .addProperty("error_handler", StaticTestingErrorHandler.class.getName())
               .addProperty("lucene_version", "LUCENE_CURRENT");
         cacheCfg.clustering().stateTransfer().fetchInMemoryState(true);
         holder.newConfigurationBuilder(defaultName).read(cacheCfg.build());

         Configuration cacheCfg1 = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false)
               .clustering().stateTransfer().fetchInMemoryState(true).build();
         holder.newConfigurationBuilder(InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME).read(cacheCfg1);
         holder.newConfigurationBuilder(InfinispanIntegration.DEFAULT_LOCKING_CACHENAME).read(cacheCfg1);
      }, NUM_NODES);

      caches.addAll(caches());
   }

   protected void verifyFindsCar(Cache cache, int count, String carMake) throws Exception {
      QueryParser queryParser = createQueryParser("make");

      Query luceneQuery = queryParser.parse(carMake);
      CacheQuery<?> cacheQuery = Search.getSearchManager(cache).getQuery(luceneQuery, Car.class);

      assertEquals(count, cacheQuery.getResultSize());

      StaticTestingErrorHandler.assertAllGood(cache);
   }

}
