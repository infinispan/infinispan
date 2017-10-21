package org.infinispan.query.distributed;

import org.testng.annotations.Test;

/**
 * Tests for Mass Indexing with both index caches and data cache stored off-heap.
 *
 * @since 9.2
 */
@Test(groups = "functional", testName = "query.distributed.MassIndexingAllOffHeapTest")
public class MassIndexingAllOffHeapTest extends DistributedMassIndexingTest {

   @Override
   protected String getConfigurationFile() {
      return "mass-index-offheap-all.xml";
   }

}
