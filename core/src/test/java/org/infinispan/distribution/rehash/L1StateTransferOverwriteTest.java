package org.infinispan.distribution.rehash;

import org.testng.annotations.Test;

/**
 * Test that ensures that state transfer values aren't overridden with L1 enabled.
 *
 * @author William Burns
 * @since 6.0
 */
@Test(groups = "functional", testName = "distribution.rehash.L1StateTransferOverwriteTest")
public class L1StateTransferOverwriteTest extends StateTransferOverwriteTest {
   public L1StateTransferOverwriteTest() {
      super();
      l1CacheEnabled = true;
   }
}
