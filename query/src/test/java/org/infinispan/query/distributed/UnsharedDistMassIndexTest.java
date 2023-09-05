package org.infinispan.query.distributed;

import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.query.queries.faceting.Car;
import org.testng.annotations.Test;

/**
 * Test for MassIndexer on DIST caches with unshared infinispan indexes
 *
 * @author gustavonalle
 * @since 7.1
 */
@Test(groups = "functional", testName = "query.distributed.UnsharedDistMassIndexTest")
public class UnsharedDistMassIndexTest extends DistributedMassIndexingTest {

   @Override
   protected String getConfigurationFile() {
      return "unshared-indexing-distribution.xml";
   }

   @Override
   protected void verifyFindsCar(Cache cache, int expectedCount, String carMake) {
      String q = String.format("FROM %s WHERE make:'%s'", Car.class.getName(), carMake);
      Query cacheQuery = cache.query(q);
      assertEquals(expectedCount, cacheQuery.execute().list().size());
   }
}
