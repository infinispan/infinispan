package org.infinispan.tx.recovery;

import static org.infinispan.tx.recovery.RecoveryTestUtil.beginAndSuspendTx;
import static org.infinispan.tx.recovery.RecoveryTestUtil.commitTransaction;
import static org.infinispan.tx.recovery.RecoveryTestUtil.prepareTransaction;
import static org.testng.AssertJUnit.assertEquals;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.tx.XidImpl;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.XaTransactionTable;
import org.infinispan.transaction.xa.recovery.InDoubtTxInfo;
import org.infinispan.transaction.xa.recovery.RecoveryAwareRemoteTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryAwareTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.transaction.xa.recovery.RecoveryManagerImpl;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;


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
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .useSynchronization(false)
            .recovery().enable()
         .clustering().stateTransfer().fetchInMemoryState(false);
      createCluster(configuration, 2);
      waitForClusterToForm();
      ComponentRegistry componentRegistry = this.cache(0).getAdvancedCache().getComponentRegistry();
      XaTransactionTable txTable =
            (XaTransactionTable) componentRegistry.getComponent(TransactionTable.class);
      TestingUtil.replaceField(
            new RecoveryManagerDelegate(TestingUtil.extractComponent(cache(0), RecoveryManager.class)),
            "recoveryManager", txTable, XaTransactionTable.class);
   }

   public void testState() throws Exception {

      RecoveryManagerImpl rm1 = (RecoveryManagerImpl) advancedCache(1).getComponentRegistry().getComponent(RecoveryManager.class);
      TransactionTable tt1 = advancedCache(1).getComponentRegistry().getComponent(TransactionTable.class);
      assertEquals(rm1.getInDoubtTransactionsMap().size(), 0);
      assertEquals(tt1.getRemoteTxCount(), 0);

      EmbeddedTransaction t0 = beginAndSuspendTx(cache(0));
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
      public CompletionStage<Void> removeRecoveryInformation(Collection<Address> where, XidImpl xid, GlobalTransaction gtx, boolean fromCluster) {
         if (swallowRemoveRecoveryInfoCalls){
            log.trace("PostCommitRecoveryStateTest$RecoveryManagerDelegate.removeRecoveryInformation");
            return CompletableFutures.completedNull();
         } else {
            return this.rm.removeRecoveryInformation(where, xid, null, false);
         }
      }

      @Override
      public CompletionStage<Void> removeRecoveryInformationFromCluster(Collection<Address> where, long internalId) {
         return rm.removeRecoveryInformationFromCluster(where, internalId);
      }

      @Override
      public RecoveryAwareTransaction removeRecoveryInformation(XidImpl xid) {
         rm.removeRecoveryInformation(xid);
         return null;
      }

      @Override
      public void registerInDoubtTransaction(RecoveryAwareRemoteTransaction tx) {
         rm.registerInDoubtTransaction(tx);
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
      public List<XidImpl> getInDoubtTransactions() {
         return rm.getInDoubtTransactions();
      }

      @Override
      public RecoveryAwareTransaction getPreparedTransaction(XidImpl xid) {
         return rm.getPreparedTransaction(xid);
      }

      @Override
      public CompletionStage<String> forceTransactionCompletion(XidImpl xid, boolean commit) {
         return rm.forceTransactionCompletion(xid, commit);
      }

      @Override
      public String forceTransactionCompletionFromCluster(XidImpl xid, Address where, boolean commit) {
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
