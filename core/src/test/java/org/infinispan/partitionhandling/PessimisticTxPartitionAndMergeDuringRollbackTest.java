package org.infinispan.partitionhandling;

import org.infinispan.Cache;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TransactionBoundaryCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

/**
 * It tests multiple scenarios where a split can happen during a transaction.
 *
 * @author Pedro Ruivo
 * @since 8.0
 */
@Test(groups = "functional", testName = "partitionhandling.PessimisticTxPartitionAndMergeDuringRollbackTest")
public class PessimisticTxPartitionAndMergeDuringRollbackTest extends BasePessimisticTxPartitionAndMergeTest {

   private static final Log log = LogFactory.getLog(PessimisticTxPartitionAndMergeDuringRollbackTest.class);

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
      //split happens before the commit() or rollback(). Locks are acquired
      waitForClusterToForm(PESSIMISTIC_TX_CACHE_NAME);

      final KeyInfo keyInfo = createKeys(PESSIMISTIC_TX_CACHE_NAME);
      final Cache<Object, String> originator = cache(0, PESSIMISTIC_TX_CACHE_NAME);

      final TransactionManager transactionManager = originator.getAdvancedCache().getTransactionManager();
      transactionManager.begin();
      keyInfo.putFinalValue(originator);
      final Transaction transaction = transactionManager.suspend();

      SplitMode.BOTH_DEGRADED.split(this);

      transactionManager.resume(transaction);
      transactionManager.rollback();

      assertEventuallyNotLocked(cache(1, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
      assertLocked(cache(2, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());

      mergeCluster();
      finalAsserts(PESSIMISTIC_TX_CACHE_NAME, keyInfo, INITIAL_VALUE);
   }

   @Override
   protected void checkLocksDuringPartition(SplitMode splitMode, KeyInfo keyInfo, boolean discard) {
      if (splitMode == SplitMode.ORIGINATOR_ISOLATED) {
         if (discard) {
            //rollback is never received.
            assertLocked(cache(1, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
         } else {
            //rollback can be delivered
            assertEventuallyNotLocked(cache(1, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
         }
      } else {
         //key is unlocked because the rollback is always received in cache1
         assertEventuallyNotLocked(cache(1, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
      }
      if (discard) {
         //rollback never received, so key is locked until the merge occurs.
         assertLocked(cache(2, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());
      } else {
         //rollback received, so key is unlocked
         assertEventuallyNotLocked(cache(2, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());
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
