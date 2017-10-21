package org.infinispan.query.distributed;

import org.testng.annotations.Test;

/**
 * Tests for Mass Indexing where all index caches are stored on-heap, but cache data is off-heap.
 *
 * @since 9.2
 */
@Test(groups = "functional", testName = "query.distributed.MassIndexingOffHeapDataOnlyTest")
public class MassIndexingOffHeapDataOnlyTest extends DistributedMassIndexingTest {

   @Override
   protected String getConfigurationFile() {
      return "mass-index-offheap-data-only.xml";
   }

}
