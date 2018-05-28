package org.infinispan.query.distributed;

import org.infinispan.Cache;
import org.infinispan.query.queries.faceting.Car;
import org.testng.annotations.Test;

/**
 * Test for MassIndexer with a store
 *
 * @author gustavonalle
 * @since 7.1
 */
@Test(groups = "functional", testName = "query.distributed.MassIndexingWithStoreTest")
public class MassIndexingWithStoreTest extends DistributedMassIndexingTest {

   @Override
   protected String getConfigurationFile() {
      return "mass-index-with-store.xml";
   }

   @Override
   public void testReindexing() throws Exception {
      Cache<String, Car> cache0 = caches.get(0);
      for (int i = 0; i < 10; i++) {
         cache0.put("CAR#" + i, new Car("Volkswagen", "white", 200));
      }
      rebuildIndexes();
      verifyFindsCar(10, "Volkswagen");
   }

}
