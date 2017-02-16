package org.infinispan.query.distributed;

import org.testng.annotations.Test;

/**
 * Test the mass indexer on a REPL cache and shared index
 * @author gustavonalle
 * @since 7.1
 */
@Test(groups = "functional", testName = "query.distributed.SharedReplMassIndexTest")
public class SharedReplMassIndexTest extends DistributedMassIndexingTest {

   protected String getConfigurationFile() {
      return "shared-repl-mass-index.xml";
   }
}
