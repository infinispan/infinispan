package org.infinispan.partitionhandling;

import org.infinispan.commands.tx.PrepareCommand;
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
@Test(groups = "functional", testName = "partitionhandling.OptimisticTxPartitionAndMergeDuringPrepareTest")
public class OptimisticTxPartitionAndMergeDuringPrepareTest extends BaseOptimisticTxPartitionAndMergeTest {

   private static final Log log = LogFactory.getLog(OptimisticTxPartitionAndMergeDuringPrepareTest.class);

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
            //they assume that the originator has crashed, so the prepare is never processed.
            assertEventuallyNotLocked(cache(1, OPTIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
            break;
         case PRIMARY_OWNER_ISOLATED:
            //the originator can recover and will retry the prepare command until it succeeds.
            assertEventuallyNotLocked(cache(1, OPTIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
            break;
         case BOTH_DEGRADED:
            //with the new changes, the rollback succeeds on the originator partition. Cache1 releases the lock.
            assertEventuallyNotLocked(cache(1, OPTIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
            break;
      }
      //or the prepare is never received, so key never locked, or it is received and it decides to rollback the transaction.
      assertEventuallyNotLocked(cache(2, OPTIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());
   }

   @Override
   protected boolean forceRollback() {
      return false;
   }

   @Override
   protected Class<? extends TransactionBoundaryCommand> getCommandClass() {
      return PrepareCommand.class;
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
