package org.infinispan.tx.recovery.admin;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.InvocationContextInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.tx.recovery.RecoveryDummyTransactionManagerLookup;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.infinispan.tx.recovery.RecoveryTestUtil.*;
import static org.testng.Assert.assertEquals;

/**
 * This test makes sure that when a transaction fails during commit it is reported as in-doubt transaction.
 *
 * @author Mircea Markus
 * @since 5.0
 */
@Test(groups = "functional", testName = "tx.recovery.admin.InDoubtWithCommitFailsTest")
@CleanupAfterMethod
public class InDoubtWithCommitFailsTest extends AbstractRecoveryTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configuration = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      configuration.transaction().transactionManagerLookup(new RecoveryDummyTransactionManagerLookup())
            .useSynchronization(false)
            .recovery().enable()
            .locking().useLockStriping(false)
            .clustering().hash().numOwners(2)
            .clustering().l1().disable().stateTransfer().fetchInMemoryState(false);
      createCluster(configuration, 2);
      waitForClusterToForm();
      advancedCache(1).addInterceptorBefore(new ForceFailureInterceptor(), InvocationContextInterceptor.class);
   }

   public void testRecoveryInfoListCommit() throws Exception {
      test(true);
   }

   public void testRecoveryInfoListRollback() throws Exception {
      test(false);
   }

   private void test(boolean commit) {
      assert recoveryOps(0).showInDoubtTransactions().isEmpty();
      TransactionTable tt0 = cache(0).getAdvancedCache().getComponentRegistry().getComponent(TransactionTable.class);

      DummyTransaction dummyTransaction = beginAndSuspendTx(cache(0));
      prepareTransaction(dummyTransaction);
      assert tt0.getLocalTxCount() == 1;

      try {
         if (commit) {
            commitTransaction(dummyTransaction);
         } else {
            rollbackTransaction(dummyTransaction);
         }
         assert false : "exception expected";
      } catch (Exception e) {
         //expected
      }
      assertEquals(tt0.getLocalTxCount(), 1);


      assertEquals(countInDoubtTx(recoveryOps(0).showInDoubtTransactions()), 1);
      assertEquals(countInDoubtTx(recoveryOps(1).showInDoubtTransactions()), 1);
   }

   public static class ForceFailureInterceptor extends CommandInterceptor {

      public AtomicBoolean fail = new AtomicBoolean(true);

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         fail();
         return super.visitCommitCommand(ctx, command);
      }

      public void fail(boolean fail) {
         this.fail.set(fail);
      }

      private void fail() {
         if (fail.get()) throw new CacheException("Induced failure");
      }

      @Override
      public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
         fail();
         return super.visitRollbackCommand(ctx, command);
      }

   }
}
