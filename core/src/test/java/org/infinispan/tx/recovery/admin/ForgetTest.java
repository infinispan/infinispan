package org.infinispan.tx.recovery.admin;

import static org.infinispan.tx.recovery.RecoveryTestUtil.beginAndSuspendTx;
import static org.infinispan.tx.recovery.RecoveryTestUtil.commitTransaction;
import static org.infinispan.tx.recovery.RecoveryTestUtil.prepareTransaction;
import static org.junit.jupiter.api.Assertions.assertEquals;

import javax.transaction.xa.XAException;

import org.infinispan.commons.tx.XidImpl;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.XaTransactionTable;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.tx.recovery.PostCommitRecoveryStateTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.recovery.admin.ForgetTest")
public class ForgetTest extends AbstractRecoveryTest {

   private EmbeddedTransaction tx;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configuration = defaultRecoveryConfig();
      createCluster(configuration, 2);
      waitForClusterToForm();

      XaTransactionTable txTable = tt(0);
      PostCommitRecoveryStateTest.RecoveryManagerDelegate recoveryManager = new PostCommitRecoveryStateTest.RecoveryManagerDelegate(
            TestingUtil.extractComponent(cache(0), RecoveryManager.class));
      TestingUtil.replaceField(recoveryManager, "recoveryManager", txTable, XaTransactionTable.class);
   }

   @BeforeMethod
   public void runTx() throws XAException {
      tx = beginAndSuspendTx(cache(0));
      prepareTransaction(tx);

      assertEquals(1, recoveryManager(0).getPreparedTransactionsFromCluster().all().length);
      assertEquals(1, tt(0).getLocalPreparedXids().size());
      assertEquals(1, tt(1).getRemoteTxCount());

      commitTransaction(tx);

      assertEquals(1, tt(1).getRemoteTxCount());
   }

   public void testInternalIdOnSameNode() {
      XidImpl xid = tx.getXid();
      recoveryOps(0).forget(xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier());
      assertEquals(0, tt(1).getRemoteTxCount());//make sure tx has been removed
   }

   public void testForgetXidOnSameNode() {
      forgetWithXid(0);
   }

   public void testForgetXidOnOtherNode() {
      forgetWithXid(1);
   }

   public void testForgetInternalIdOnSameNode() {
      forgetWithInternalId(0);
   }

   public void testForgetInternalIdOnOtherNode() {
      forgetWithInternalId(1);
   }

   protected void forgetWithInternalId(int cacheIndex) {
      long internalId = -1;
      for (RemoteTransaction rt : tt(1).getRemoteTransactions()) {
         GlobalTransaction a = rt.getGlobalTransaction();
         if (a.getXid().equals(tx.getXid())) {
            internalId = a.getInternalId();
         }
      }
      if (internalId == -1) throw new IllegalStateException();
      log.tracef("About to forget... %s", internalId);
      recoveryOps(cacheIndex).forget(internalId);
      assertEquals(0, tt(0).getRemoteTxCount());
      assertEquals(0, tt(1).getRemoteTxCount());
   }


   private void forgetWithXid(int nodeIndex) {
      XidImpl xid = tx.getXid();
      recoveryOps(nodeIndex).forget(xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier());
      assertEquals(0, tt(1).getRemoteTxCount());//make sure tx has been removed
   }
}
