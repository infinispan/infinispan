package org.infinispan.distribution.rehash;

import org.testng.annotations.Test;

/**
 * Test that ensure when L1 cache is disabled that if writes occurs during a state transfer and vice versa that the
 * proper data is available.
 *
 * @author William Burns
 * @since 6.0
 */
@Test(groups = "functional", testName = "distribution.rehash.TxNonL1StateTransferOverwriteTest")
public class TxNonL1StateTransferOverwriteTest extends BaseTxStateTransferOverwriteTest {
   public TxNonL1StateTransferOverwriteTest() {
      super();
      l1CacheEnabled = false;
   }

   public void testNonCoordinatorOwnerLeavingDuringReplace() throws Exception {
      doTestWhereCommitOccursAfterStateTransferBeginsBeforeCompletion(TestWriteOperation.REPLACE);
   }

   public void testNonCoordinatorOwnerLeavingDuringReplaceExact() throws Exception {
      doTestWhereCommitOccursAfterStateTransferBeginsBeforeCompletion(TestWriteOperation.REPLACE_EXACT);
   }

   public void testNonCoordinatorOwnerLeavingDuringRemove() throws Exception {
      doTestWhereCommitOccursAfterStateTransferBeginsBeforeCompletion(TestWriteOperation.REMOVE);
   }

   public void testNonCoordinatorOwnerLeavingDuringRemoveExact() throws Exception {
      doTestWhereCommitOccursAfterStateTransferBeginsBeforeCompletion(TestWriteOperation.REMOVE_EXACT);
   }

   public void testNonCoordinatorOwnerLeavingDuringPutOverwrite() throws Exception {
      doTestWhereCommitOccursAfterStateTransferBeginsBeforeCompletion(TestWriteOperation.PUT_OVERWRITE);
   }
}
