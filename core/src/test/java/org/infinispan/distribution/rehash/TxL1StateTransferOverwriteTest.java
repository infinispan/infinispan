package org.infinispan.distribution.rehash;

import org.testng.annotations.Test;

/**
 * Test that ensures that state transfer values aren't overridden with a tx cache with L1 enabled.
 *
 * @author William Burns
 * @since 6.0
 */
@Test(groups = "functional", testName = "distribution.rehash.TxL1StateTransferOverwriteTest")
public class TxL1StateTransferOverwriteTest extends BaseTxStateTransferOverwriteTest {
   public TxL1StateTransferOverwriteTest() {
      super();
      l1CacheEnabled = true;
   }
}
