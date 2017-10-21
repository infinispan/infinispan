package org.infinispan.query.distributed;

import org.testng.annotations.Test;

/**
 * Tests for Mass Indexing with data cache on heap, but index caches off-heap.
 *
 * @since 9.2
 */
@Test(groups = "functional", testName = "query.distributed.MassIndexingOffHeapIndexOnlyTest")
public class MassIndexingOffHeapIndexOnlyTest extends DistributedMassIndexingTest {

   @Override
   protected String getConfigurationFile() {
      return "mass-index-offheap-index-only.xml";
   }

}
