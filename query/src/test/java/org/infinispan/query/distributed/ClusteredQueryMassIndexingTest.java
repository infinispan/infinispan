package org.infinispan.query.distributed;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.IndexedQueryMode;
import org.infinispan.query.queries.faceting.Car;
import org.testng.annotations.Test;

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
      String q = String.format("FROM %s WHERE make:'%s'", Car.class.getName(), carMake);
      CacheQuery<?> cacheQuery = Search.getSearchManager(cache)
            .getQuery(q, IndexedQueryMode.BROADCAST);

      assertEquals(expectedCount, cacheQuery.getResultSize());
   }

}
