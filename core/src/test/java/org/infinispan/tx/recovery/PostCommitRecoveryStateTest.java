package org.infinispan.tx.recovery;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.XaTransactionTable;
import org.infinispan.transaction.xa.recovery.RecoveryAwareTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.transaction.xa.recovery.RecoveryManagerImpl;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import javax.transaction.xa.Xid;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.infinispan.tx.recovery.RecoveryTestUtil.beginAndSuspendTx;
import static org.infinispan.tx.recovery.RecoveryTestUtil.commitTransaction;
import static org.infinispan.tx.recovery.RecoveryTestUtil.prepareTransaction;


/**
 * @author Mircea Markus
 * @since 5.0
 */
@Test (groups = "functional", testName = "tx.recovery.PostCommitRecoveryStateTest")
public class PostCommitRecoveryStateTest extends MultipleCacheManagersTest {

   private static Log log = LogFactory.getLog(PostCommitRecoveryStateTest.class);

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configuration = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      configuration
         .locking().useLockStriping(false)
         .transaction()
            .transactionManagerLookup(new RecoveryDummyTransactionManagerLookup())
            .useSynchronization(false)
            .recovery().enable()
         .clustering().stateTransfer().fetchInMemoryState(false);
      createCluster(configuration, 2);
      waitForClusterToForm();
      ComponentRegistry componentRegistry = this.cache(0).getAdvancedCache().getComponentRegistry();
      XaTransactionTable txTable = (XaTransactionTable) componentRegistry.getComponent(TransactionTable.class);
      txTable.setRecoveryManager(new RecoveryManagerDelegate(txTable.getRecoveryManager()));
   }

   public void testState() throws Exception {

      RecoveryManagerImpl rm1 = (RecoveryManagerImpl) advancedCache(1).getComponentRegistry().getComponent(RecoveryManager.class);
      TransactionTable tt1 = advancedCache(1).getComponentRegistry().getComponent(TransactionTable.class);
      assertEquals(rm1.getInDoubtTransactionsMap().size(), 0);
      assertEquals(tt1.getRemoteTxCount(), 0);

      DummyTransaction t0 = beginAndSuspendTx(cache(0));
      assertEquals(rm1.getInDoubtTransactionsMap().size(),0);
      assertEquals(tt1.getRemoteTxCount(), 0);

      prepareTransaction(t0);
      assertEquals(rm1.getInDoubtTransactionsMap().size(),0);
      assertEquals(tt1.getRemoteTxCount(), 1);

      commitTransaction(t0);
      assertEquals(tt1.getRemoteTxCount(), 1);
      assertEquals(rm1.getInDoubtTransactionsMap().size(), 0);
   }

   public static class RecoveryManagerDelegate implements RecoveryManager {
      volatile RecoveryManager rm;

      public boolean swallowRemoveRecoveryInfoCalls = true;

      public RecoveryManagerDelegate(RecoveryManager recoveryManager) {
         this.rm = recoveryManager;
      }

      @Override
      public RecoveryIterator getPreparedTransactionsFromCluster() {
         return rm.getPreparedTransactionsFromCluster();
      }

      @Override
      public void removeRecoveryInformationFromCluster(Collection<Address> where, Xid xid, boolean sync, GlobalTransaction gtx) {
         if (swallowRemoveRecoveryInfoCalls){
            log.trace("PostCommitRecoveryStateTest$RecoveryManagerDelegate.removeRecoveryInformation");
         } else {
            this.rm.removeRecoveryInformationFromCluster(where, xid, sync, null);
         }
      }

      @Override
      public void removeRecoveryInformationFromCluster(Collection<Address> where, long internalId, boolean sync) {
         rm.removeRecoveryInformationFromCluster(where, internalId, sync);
      }

      @Override
      public RecoveryAwareTransaction removeRecoveryInformation(Xid xid) {
         rm.removeRecoveryInformation(xid);
         return null;
      }

      @Override
      public Set<InDoubtTxInfo> getInDoubtTransactionInfoFromCluster() {
         return rm.getInDoubtTransactionInfoFromCluster();
      }

      @Override
      public Set<InDoubtTxInfo> getInDoubtTransactionInfo() {
         return rm.getInDoubtTransactionInfo();
      }

      @Override
      public List<Xid> getInDoubtTransactions() {
         return rm.getInDoubtTransactions();
      }

      @Override
      public RecoveryAwareTransaction getPreparedTransaction(Xid xid) {
         return rm.getPreparedTransaction(xid);
      }

      @Override
      public String forceTransactionCompletion(Xid xid, boolean commit) {
         return rm.forceTransactionCompletion(xid, commit);
      }

      @Override
      public String forceTransactionCompletionFromCluster(Xid xid, Address where, boolean commit) {
         return rm.forceTransactionCompletionFromCluster(xid, where, commit);
      }

      @Override
      public boolean isTransactionPrepared(GlobalTransaction globalTx) {
         return rm.isTransactionPrepared(globalTx);
      }

      @Override
      public RecoveryAwareTransaction removeRecoveryInformation(Long internalId) {
         rm.removeRecoveryInformation(internalId);
         return null;
      }
   }
}
