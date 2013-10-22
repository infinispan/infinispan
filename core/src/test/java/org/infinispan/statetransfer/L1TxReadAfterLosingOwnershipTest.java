package org.infinispan.statetransfer;

import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * Tests the read when a node loses the ownership of a key for transactional caches with L1 enabled.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "statetransfer.L1TxReadAfterLosingOwnershipTest")
@CleanupAfterMethod
public class L1TxReadAfterLosingOwnershipTest extends TxReadAfterLosingOwnershipTest {

   @Override
   protected boolean l1() {
      return true;
   }
}
