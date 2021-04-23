package org.infinispan.query.distributed;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.query.Search;
import org.infinispan.query.dsl.Query;
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
      Query cacheQuery = Search.getQueryFactory(cache).create(q);

      assertEquals(expectedCount, cacheQuery.execute().list().size());
   }

}
