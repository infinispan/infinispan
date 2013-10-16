package org.infinispan.distribution.rehash;

import org.testng.annotations.Test;

/**
 * Test that ensure when L1 cache is enabled that if writes occurs during a state transfer and vice versa that the
 * proper data is available.
 *
 * @author William Burns
 * @since 6.0
 */
@Test(groups = "functional", testName = "distribution.rehash.TxL1StateTransferOverwriteTest")
public class TxL1StateTransferOverwriteTest extends BaseTxStateTransferOverwriteTest {
   public TxL1StateTransferOverwriteTest() {
      super();
      l1CacheEnabled = true;
      l1OnRehash = true;
   }
}
