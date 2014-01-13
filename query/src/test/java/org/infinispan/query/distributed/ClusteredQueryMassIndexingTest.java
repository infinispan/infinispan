package org.infinispan.query.distributed;

import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.Query;
import org.infinispan.Cache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.queries.faceting.Car;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.infinispan.query.helper.TestQueryHelperFactory.createQueryParser;

/**
 * Tests verifying that the Mass Indexing works for Clustered queries as well.
 *
 * @TODO enable the test when ISPN-2661 is fixed.
 */
@Test(groups = "functional", testName = "query.distributed.ClusteredQueryMassIndexingTest")
public class ClusteredQueryMassIndexingTest extends DistributedMassIndexingTest {

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

   //@TODO remove when ISPN-2661 is fixed.
   @Test(groups = "unstable")
   public void testReindexing() throws Exception {
      super.testReindexing();
   }
}
