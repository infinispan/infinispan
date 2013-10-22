package org.infinispan.statetransfer;

import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.Test;

/**
 * Tests the read when a node loses the ownership of a key for non transactional classes
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "statetransfer.NonTxReadAfterLosingOwnershipTest")
@CleanupAfterMethod
public class NonTxReadAfterLosingOwnershipTest extends TxReadAfterLosingOwnershipTest {

   @Override
   protected boolean transactional() {
      return false;
   }
}
