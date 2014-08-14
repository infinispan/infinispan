package org.infinispan.query.distributed;

import org.testng.annotations.Test;

/**
 * The same as the MultiNodeDistributedTest, only the cache configuration is transactional.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.distributed.MultiNodeDistributedTxTest")
public class MultiNodeDistributedTxTest extends MultiNodeDistributedTest {

   protected boolean transactionsEnabled() {
      return true;
   }

   @Override
   protected String getConfigurationResourceName() {
      return "dynamic-transactional-indexing-distribution.xml";
   }

}
