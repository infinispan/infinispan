package org.infinispan.distribution.rehash;

import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnComponentMethod;
import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnGlobalComponentMethod;
import static org.infinispan.test.concurrent.StateSequencerUtil.matchMethodCall;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.inboundhandler.OffloadInboundInvocationHandler;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.concurrent.InvocationMatcher;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Tests that state transfer properly replicates locks in a pessimistic cache, when the originator of the transaction
 * is/was the primary owner.
 * <p>
 * See ISPN-4091, ISPN-4108
 *
 * @author Dan Berindei
 * @since 7.0
 */
@Test(groups = "functional", testName = "distribution.rehash.PessimisticStateTransferLocksTest")
public class PessimisticStateTransferLocksTest extends MultipleCacheManagersTest {

   private static final String KEY = "key";
   private static final String VALUE = "value";

   {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   private StateSequencer sequencer;
   private ControlledConsistentHashFactory<?> consistentHashFactory;

   @AfterMethod(alwaysRun = true)
   public void printSequencerState() {
      log.debugf("Sequencer state: %s", sequencer);
      if (sequencer != null) {
         sequencer.stop();
         sequencer = null;
      }
   }

   @SuppressWarnings("resource")
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getConfigurationBuilder();
      createCluster(ControlledConsistentHashFactory.SCI.INSTANCE, c, 3);
      waitForClusterToForm();
      cacheManagers.forEach(OffloadInboundInvocationHandler::replaceOn);
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      consistentHashFactory = new ControlledConsistentHashFactory.Default(0, 1);
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.DIST_SYNC);
      c.clustering().hash().consistentHashFactory(consistentHashFactory).numSegments(1);
      c.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      c.transaction().lockingMode(LockingMode.PESSIMISTIC);
      return c;
   }

   public void testPutStartedBeforeRebalance() throws Exception {
      sequencer = new StateSequencer();
      sequencer.logicalThread("tx", "tx:perform_op", "tx:check_locks", "tx:before_commit", "tx:after_commit");
      sequencer.logicalThread("rebalance", "rebalance:before_get_tx", "rebalance:after_get_tx",
            "rebalance:before_confirm", "rebalance:end");
      sequencer.order("tx:perform_op", "rebalance:before_get_tx", "rebalance:after_get_tx", "tx:check_locks",
            "rebalance:before_confirm", "rebalance:end", "tx:before_commit");

      startTxWithPut();
      startRebalance();
      checkLocksBeforeCommit(false);
      waitRebalanceEnd();
      endTx();
      checkLocksAfterCommit();
   }

   public void testLockStartedBeforeRebalance() throws Exception {
      sequencer = new StateSequencer();
      sequencer.logicalThread("tx", "tx:perform_op", "tx:check_locks", "tx:before_commit", "tx:after_commit");
      sequencer.logicalThread("rebalance", "rebalance:before_get_tx", "rebalance:after_get_tx",
            "rebalance:before_confirm", "rebalance:end");
      sequencer.order("tx:perform_op", "rebalance:before_get_tx", "rebalance:after_get_tx", "tx:check_locks",
            "rebalance:before_confirm", "rebalance:end", "tx:before_commit");

      startTxWithLock();
      startRebalance();
      checkLocksBeforeCommit(false);
      waitRebalanceEnd();
      endTx();
      checkLocksAfterCommit();
   }

   public void testPutStartedDuringRebalance() throws Exception {
      sequencer = new StateSequencer();
      sequencer.logicalThread("tx", "tx:perform_op", "tx:check_locks", "tx:before_commit",
            "tx:after_commit");
      sequencer.logicalThread("rebalance", "rebalance:before_get_tx", "rebalance:after_get_tx",
            "rebalance:before_confirm", "rebalance:end");
      sequencer.order("rebalance:after_get_tx", "tx:perform_op", "tx:check_locks",
            "rebalance:before_confirm", "rebalance:end", "tx:before_commit");

      startRebalance();
      startTxWithPut();
      checkLocksBeforeCommit(true);
      waitRebalanceEnd();
      endTx();
      checkLocksAfterCommit();
   }

   public void testLockStartedDuringRebalance() throws Exception {
      sequencer = new StateSequencer();
      sequencer.logicalThread("tx", "tx:perform_op", "tx:check_locks", "tx:before_commit", "tx:after_commit");
      sequencer.logicalThread("rebalance", "rebalance:before_get_tx", "rebalance:after_get_tx",
            "rebalance:before_confirm", "rebalance:end");
      sequencer.order("rebalance:after_get_tx", "tx:perform_op", "tx:check_locks",
            "rebalance:before_confirm", "rebalance:end", "tx:before_commit");

      startRebalance();
      startTxWithLock();
      checkLocksBeforeCommit(true);
      waitRebalanceEnd();
      endTx();
      checkLocksAfterCommit();
   }

   private void startTxWithPut() throws Exception {
      sequencer.enter("tx:perform_op");
      tm(0).begin();
      cache(0).put(KEY, VALUE);
      sequencer.exit("tx:perform_op");
   }

   private void startTxWithLock() throws Exception {
      sequencer.enter("tx:perform_op");
      tm(0).begin();
      advancedCache(0).lock(KEY);
      sequencer.exit("tx:perform_op");
   }

   private void startRebalance() throws Exception {
      InvocationMatcher rebalanceCompletedMatcher = matchMethodCall("handleRebalancePhaseConfirm")
            .withParam(1, address(2)).matchCount(0).build();
      advanceOnGlobalComponentMethod(sequencer, manager(0), ClusterTopologyManager.class, rebalanceCompletedMatcher)
            .before("rebalance:before_confirm");

      InvocationMatcher localRebalanceMatcher = matchMethodCall("onTopologyUpdate")
            .withParam(1, true).matchCount(0).build();
      advanceOnComponentMethod(sequencer, cache(2), StateConsumer.class, localRebalanceMatcher)
            .before("rebalance:before_get_tx").afterAsync("rebalance:after_get_tx");
      consistentHashFactory.setOwnerIndexes(2, 1);
      consistentHashFactory.triggerRebalance(cache(0));
   }

   private void waitRebalanceEnd() throws Exception {
      sequencer.advance("rebalance:end");
      TestingUtil.waitForNoRebalance(caches());
   }

   private void endTx() throws Exception {
      sequencer.advance("tx:before_commit");
      tm(0).commit();
   }

   private void checkLocksBeforeCommit(boolean backupLockOnCache1) throws Exception {
      sequencer.enter("tx:check_locks");
      assertFalse(getTransactionTable(cache(0)).getLocalTransactions().isEmpty());
      assertTrue(getTransactionTable(cache(0)).getRemoteTransactions().isEmpty());
      LocalTransaction localTx = getTransactionTable(cache(0)).getLocalTransactions().iterator().next();
      assertEquals(Collections.singleton(KEY), localTx.getLockedKeys());
      assertEquals(Collections.emptySet(), localTx.getBackupLockedKeys());

      assertTrue(getTransactionTable(cache(1)).getLocalTransactions().isEmpty());
      assertEquals(backupLockOnCache1, !getTransactionTable(cache(1)).getRemoteTransactions().isEmpty());

      assertTrue(getTransactionTable(cache(2)).getLocalTransactions().isEmpty());
      assertFalse(getTransactionTable(cache(2)).getRemoteTransactions().isEmpty());
      RemoteTransaction remoteTx = getTransactionTable(cache(2)).getRemoteTransactions().iterator().next();
      assertEquals(Collections.emptySet(), remoteTx.getLockedKeys());
      assertEquals(Collections.singleton(KEY), remoteTx.getBackupLockedKeys());
      sequencer.exit("tx:check_locks");
   }

   private void checkLocksAfterCommit() {
      for (Cache<Object, Object> c : caches()) {
         final TransactionTable txTable = getTransactionTable(c);
         assertTrue(txTable.getLocalTransactions().isEmpty());
         eventuallyEquals(0, () -> txTable.getRemoteTransactions().size());
      }
   }

   private TransactionTable getTransactionTable(Cache<Object, Object> c) {
      return TestingUtil.extractComponent(c, TransactionTable.class);
   }
}
