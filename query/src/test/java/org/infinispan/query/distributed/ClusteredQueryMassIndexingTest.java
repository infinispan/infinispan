package org.infinispan.query.distributed;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.infinispan.Cache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.queries.faceting.Car;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.infinispan.query.helper.TestQueryHelperFactory.createQueryParser;

/**
 * Tests verifying that the Mass Indexing works for Clustered queries as well.
 */
@Test(groups = "functional", testName = "query.distributed.ClusteredQueryMassIndexingTest")
public class ClusteredQueryMassIndexingTest extends DistributedMassIndexingTest {

   @Override
   protected String getConfigurationFile() {
      return "unshared-indexing-distribution.xml";
   }

   protected void verifyFindsCar(Cache cache, int expectedCount, String carMake) {
      QueryParser queryParser = createQueryParser("make");

      try {
         Query luceneQuery = queryParser.parse(carMake);
         CacheQuery cacheQuery = Search.getSearchManager(cache).getClusteredQuery(luceneQuery, Car.class);

         Assert.assertEquals(expectedCount, cacheQuery.getResultSize());
      } catch(ParseException ex) {
         ex.printStackTrace();
         Assert.fail("Failed due to: " + ex.getMessage());
      }
   }

   @Override
   protected void rebuildIndexes() throws Exception {
      Cache cache = caches.get(0);
      SearchManager searchManager = Search.getSearchManager(cache);
      MassIndexer massIndexer = searchManager.getMassIndexer();
      massIndexer.setFlushPerNodeEnabled(true);
      massIndexer.start();
   }
}
