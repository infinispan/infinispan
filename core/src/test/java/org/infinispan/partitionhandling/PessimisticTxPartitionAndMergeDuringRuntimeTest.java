package org.infinispan.partitionhandling;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * It tests multiple scenarios where a split can happen during a transaction.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
@Test(groups = "functional", testName = "partitionhandling.PessimisticTxPartitionAndMergeDuringRuntimeTest")
public class PessimisticTxPartitionAndMergeDuringRuntimeTest extends BasePessimisticTxPartitionAndMergeTest {

   private static final Log log = LogFactory.getLog(PessimisticTxPartitionAndMergeDuringRuntimeTest.class);

   public void testDegradedPartitionWithDiscard() throws Exception {
      doTest(SplitMode.BOTH_DEGRADED, true, true);
   }

   public void testDegradedPartition() throws Exception {
      doTest(SplitMode.BOTH_DEGRADED, true, false);
   }

   public void testOriginatorIsolatedPartitionWithDiscard() throws Exception {
      doTest(SplitMode.ORIGINATOR_ISOLATED, true, true);
   }

   public void testOriginatorIsolatedPartition() throws Exception {
      doTest(SplitMode.ORIGINATOR_ISOLATED, true, false);
   }

   public void testPrimaryOwnerIsolatedPartitionWithDiscard() throws Exception {
      doTest(SplitMode.PRIMARY_OWNER_ISOLATED, false, true);
   }

   public void testPrimaryOwnerIsolatedPartition() throws Exception {
      doTest(SplitMode.PRIMARY_OWNER_ISOLATED, false, false);
   }

   @Override
   protected void checkLocksDuringPartition(SplitMode splitMode, KeyInfo keyInfo, boolean discard) {
      switch (splitMode) {
         case ORIGINATOR_ISOLATED:
            //lock command never received or ignored because the originator is in another partition
            //(safe because we will rollback anyway)
            assertEventuallyNotLocked(cache(1, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
         case BOTH_DEGRADED:
            //originator rollbacks which is received by cache1
            assertEventuallyNotLocked(cache(1, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
         case PRIMARY_OWNER_ISOLATED:
            //originator can commit the transaction, key is eventually unlocked
            assertEventuallyNotLocked(cache(1, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
      }
      //lock command is discarded since the transaction originator is missing
      assertEventuallyNotLocked(cache(2, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());
   }

   @Override
   protected boolean forceRollback() {
      return false;
   }

   @Override
   protected Class<? extends TransactionBoundaryCommand> getCommandClass() {
      return LockControlCommand.class;
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
