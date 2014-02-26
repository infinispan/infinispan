package org.infinispan.query.distributed;

import org.infinispan.context.Flag;
import org.infinispan.query.api.NotIndexedType;
import org.infinispan.query.queries.faceting.Car;
import org.testng.annotations.Test;

/**
 * Running mass indexer on big bunch of data.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.distributed.MassIndexingTest")
public class MassIndexingTest extends DistributedMassIndexingTest {

   @Test(groups = "unstable", description = "See ISPN-4043")
   public void testReindexing() throws Exception {
      for(int i = 0; i < 200; i++) {
         caches.get(i % 2).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).put(key("F" + i + "NUM"),
                                                                                new Car((i % 2 == 0 ? "megane" : "bmw"), "blue", 300 + i));
      }

      //Adding also non-indexed values
      caches.get(0).getAdvancedCache().put(key("FNonIndexed1NUM"), new NotIndexedType("test1"));
      caches.get(0).getAdvancedCache().put(key("FNonIndexed2NUM"), new NotIndexedType("test2"));

      verifyFindsCar(0, "megane");
      verifyFindsCar(0, "test1");
      verifyFindsCar(0, "test2");

      caches.get(0).getAdvancedCache().withFlags(Flag.SKIP_INDEXING).put(key("FNonIndexed3NUM"), new NotIndexedType("test3"));
      verifyFindsCar(0, "test3");

      //re-sync datacontainer with indexes:
      rebuildIndexes();

      verifyFindsCar(100, "megane");
      verifyFindsCar(0, "test1");
      verifyFindsCar(0, "test2");
   }
}
