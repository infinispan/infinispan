package org.infinispan.query.blackbox;

import org.testng.annotations.Test;

/**
 * @since 9.0
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredCacheWithAffinityIndexManagerTx")
public class ClusteredCacheWithAffinityIndexManagerTx extends ClusteredCacheWithAffinityIndexManagerTest {

   @Override
   protected boolean transactionsEnabled() {
      return true;
   }
}
