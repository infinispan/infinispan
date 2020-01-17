package org.infinispan.query.distributed;

import org.testng.annotations.Test;

/**
 * Test for MassIndexer on DIST caches with Index.ALL with local filesystem indexes.
 *
 * @since 10.1
 */
@Test(groups = "functional", testName = "query.distributed.UnsharedDistRedundantMassIndexTest")
public class UnsharedDistRedundantMassIndexTest extends UnsharedDistMassIndexTest {

   @Override
   protected String getConfigurationFile() {
      return "non-shared-redundant-index.xml";
   }
}
