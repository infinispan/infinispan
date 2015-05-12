package org.infinispan.partitionhandling;

import org.infinispan.Cache;
import org.infinispan.commands.tx.PrepareCommand;
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
@Test(groups = "functional", testName = "partitionhandling.PessimisticTxPartitionAndMergeDuringPrepareTest")
public class PessimisticTxPartitionAndMergeDuringPrepareTest extends BasePessimisticTxPartitionAndMergeTest {

   private static final Log log = LogFactory.getLog(PessimisticTxPartitionAndMergeDuringPrepareTest.class);

   public void testDegradedPartitionWithDiscard() throws Exception {
      doTest(SplitMode.BOTH_DEGRADED, false, true);
   }

   public void testDegradedPartition() throws Exception {
      doTest(SplitMode.BOTH_DEGRADED, false, false);
   }

   public void testOriginatorIsolatedPartitionWithDiscard() throws Exception {
      doTest(SplitMode.ORIGINATOR_ISOLATED, false, true);
   }

   public void testOriginatorIsolatedPartition() throws Exception {
      doTest(SplitMode.ORIGINATOR_ISOLATED, false, false);
   }

   public void testPrimaryOwnerIsolatedPartitionWithDiscard() throws Exception {
      doTest(SplitMode.PRIMARY_OWNER_ISOLATED, false, true);
   }

   public void testPrimaryOwnerIsolatedPartition() throws Exception {
      doTest(SplitMode.PRIMARY_OWNER_ISOLATED, false, false);
   }

   public void testSplitBeforePrepare() throws Exception {
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
      transactionManager.commit();

      assertLocked(cache(1, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
      assertLocked(cache(2, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());

      mergeCluster();
      finalAsserts(PESSIMISTIC_TX_CACHE_NAME, keyInfo, FINAL_VALUE);
   }

   @Override
   protected void checkLocksDuringPartition(SplitMode splitMode, KeyInfo keyInfo, boolean discard) {
      //always locked: locks acquired in runtime
      assertLocked(cache(1, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey1());
      if (discard) {
         //locks are acquired during runtime
         assertLocked(cache(2, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());
      } else {
         //prepare will succeed, but the locks are not released (the TxCompletionNotificationCommand will do it)
         assertLocked(cache(2, PESSIMISTIC_TX_CACHE_NAME), keyInfo.getKey2());
      }
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
