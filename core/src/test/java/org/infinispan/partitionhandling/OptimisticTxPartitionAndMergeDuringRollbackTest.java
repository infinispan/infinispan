package org.infinispan.partitionhandling;

import org.infinispan.Cache;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * It tests multiple scenarios where a split can happen during a transaction.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
@Test(groups = "functional", testName = "partitionhandling.OptimisticTxPartitionAndMergeDuringRollbackTest")
public class OptimisticTxPartitionAndMergeDuringRollbackTest extends BaseOptimisticTxPartitionAndMergeTest {

   private static final Log log = LogFactory.getLog(OptimisticTxPartitionAndMergeDuringRollbackTest.class);

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
      doTest(SplitMode.PRIMARY_OWNER_ISOLATED, true, true);
   }

   public void testPrimaryOwnerIsolatedPartition() throws Exception {
      doTest(SplitMode.PRIMARY_OWNER_ISOLATED, true, false);
   }

   public void testSplitBeforeRollback() throws Exception {
      //the transaction is successfully prepare and then the split happens before the commit phase starts.
      waitForClusterToForm(OPTIMISTIC_TX_CACHE_NAME);
      final KeyInfo keyInfo = createKeys(OPTIMISTIC_TX_CACHE_NAME);
      final Cache<Object, String> originator = cache(0, OPTIMISTIC_TX_CACHE_NAME);

      final DummyTransactionManager transactionManager = (DummyTransactionManager) originator.getAdvancedCache().getTransactionManager();
      transactionManager.begin();
      final DummyTransaction transaction = transactionManager.getTransaction();
      keyInfo.putFinalValue(originator);
      AssertJUnit.assertTrue(transaction.runPrepare());
      transactionManager.suspend();

      SplitMode.BOTH_DEGRADED.split(this);

      transactionManager.resume(transaction);
      transaction.runCommit(true);

      assertEventuallyNotLocked(cache(1, OPTIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
      assertLocked(cache(2, OPTIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());

      mergeCluster();
      finalAsserts(OPTIMISTIC_TX_CACHE_NAME, keyInfo, INITIAL_VALUE);
   }

   @Override
   protected void checkLocksDuringPartition(SplitMode splitMode, KeyInfo keyInfo, boolean discard) {
      if (splitMode == SplitMode.ORIGINATOR_ISOLATED && discard) {
         //rollback never received, so key is locked until the merge occurs.
         assertLocked(cache(1, OPTIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
      } else {
         //key is unlocked because the rollback is always received in cache1
         assertEventuallyNotLocked(cache(1, OPTIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
      }
      if (discard) {
         //rollback never received, so key is locked until the merge occurs.
         assertLocked(cache(2, OPTIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());
      } else {
         //rollback received, so key is unlocked
         assertEventuallyNotLocked(cache(2, OPTIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());
      }
   }

   @Override
   protected boolean forceRollback() {
      return true;
   }

   @Override
   protected Class<? extends TransactionBoundaryCommand> getCommandClass() {
      return RollbackCommand.class;
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
