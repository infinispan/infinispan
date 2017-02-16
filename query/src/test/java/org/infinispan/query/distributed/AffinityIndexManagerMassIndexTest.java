package org.infinispan.query.distributed;

import org.testng.annotations.Test;

/**
 * @author gustavonalle
 * @since 8.2
 */
@Test(groups = "functional", testName = "query.distributed.AffinityIndexManagerMassIndexTest")
public class AffinityIndexManagerMassIndexTest extends DistributedMassIndexingTest {

   protected String getConfigurationFile() {
      return "affinity-mass-index.xml";
   }

}
