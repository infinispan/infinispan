package org.infinispan.query.distributed;

import org.testng.annotations.Test;

/**
 * Similiar to MultiNodeLocalTest, only uses transactional infinispan configuration.
 *
 * @TODO enable the test when ISPN-2727 is fixed.
 *
 * @author Anna Manukyan
 */
@Test(groups = "unstable", testName = "query.distributed.MultiNodeLocalTxTest", description = "original group: functional -- ISPN-2727")
public class MultiNodeLocalTxTest extends MultiNodeLocalTest {

   public boolean transactionsEnabled() {
      return true;
   }

   //@TODO enable the test when ISPN-2727 is fixed.
   @Test(groups = "unstable")
   public void testIndexingWorkDistribution() throws Exception {
      super.testIndexingWorkDistribution();
   }
}
