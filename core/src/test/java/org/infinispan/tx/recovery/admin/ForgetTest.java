package org.infinispan.tx.recovery.admin;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.xa.XaTransactionTable;
import org.infinispan.transaction.xa.recovery.RecoverableTransactionIdentifier;
import org.infinispan.tx.recovery.PostCommitRecoveryStateTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import static junit.framework.Assert.assertEquals;
import static org.infinispan.tx.recovery.RecoveryTestUtil.*;

/**
 * @author Mircea Markus
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.recovery.admin.ForgetTest")
public class ForgetTest extends AbstractRecoveryTest {

   private PostCommitRecoveryStateTest.RecoveryManagerDelegate recoveryManager;
   private DummyTransaction tx;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configuration = defaultRecoveryConfig();
      createCluster(configuration, 2);
      waitForClusterToForm();

      XaTransactionTable txTable = tt(0);
      recoveryManager = new PostCommitRecoveryStateTest.RecoveryManagerDelegate(txTable.getRecoveryManager());
      txTable.setRecoveryManager(recoveryManager);
   }

   @BeforeMethod
   public void runTx() throws XAException {
      tx = beginAndSuspendTx(cache(0));
      prepareTransaction(tx);

      assertEquals(recoveryManager(0).getPreparedTransactionsFromCluster().all().length, 1);
      assertEquals(tt(0).getLocalPreparedXids().size(), 1);
      assertEquals(tt(1).getRemoteTxCount(), 1);

      commitTransaction(tx);

      assertEquals(tt(1).getRemoteTxCount(), 1);
   }

   public void testInternalIdOnSameNode() throws Exception {
      Xid xid = tx.getXid();
      recoveryOps(0).forget(xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier());
      assertEquals(tt(1).getRemoteTxCount(), 0);//make sure tx has been removed
   }

   public void testForgetXidOnSameNode() throws Exception {
      forgetWithXid(0);
   }

   public void testForgetXidOnOtherNode() throws Exception {
      forgetWithXid(1);
   }

   public void testForgetInternalIdOnSameNode() throws Exception {
      forgetWithInternalId(0);
   }

   public void testForgetInternalIdOnOtherNode() throws Exception {
      forgetWithInternalId(1);
   }

   protected void forgetWithInternalId(int cacheIndex) {
      long internalId = -1;
      for (RemoteTransaction rt : tt(1).getRemoteTransactions()) {
         RecoverableTransactionIdentifier a = (RecoverableTransactionIdentifier) rt.getGlobalTransaction();
         if (a.getXid().equals(tx.getXid())) {
            internalId = a.getInternalId();
         }
      }
      if (internalId == -1) throw new IllegalStateException();
      log.tracef("About to forget... %s", internalId);
      recoveryOps(cacheIndex).forget(internalId);
      assertEquals(tt(0).getRemoteTxCount(), 0);
      assertEquals(tt(1).getRemoteTxCount(), 0);
   }


   private void forgetWithXid(int nodeIndex) {
      Xid xid = tx.getXid();
      recoveryOps(nodeIndex).forget(xid.getFormatId(), xid.getGlobalTransactionId(), xid.getBranchQualifier());
      assertEquals(tt(1).getRemoteTxCount(), 0);//make sure tx has been removed
   }
}
