package org.infinispan.distribution.rehash;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.util.ControlledRpcManager.replaceRpcManager;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.remote.RevokeBiasCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.triangle.BackupWriteCommand;
import org.infinispan.commands.tx.AbstractTransactionBoundaryCommand;
import org.infinispan.commands.write.InvalidateVersionsCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.globalstate.NoOpGlobalConfigurationManager;
import org.infinispan.interceptors.AsyncInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.ControlledRpcManager;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * Tests that state transfer can't overwrite a value written by a command during state transfer.
 * See https://issues.jboss.org/browse/ISPN-3443
 *
 * @author Dan Berindei
 * @since 6.0
 */
@Test(groups = "functional", testName = "distribution.rehash.StateTransferOverwritingValueTest")
public class StateTransferOverwritingValueTest extends MultipleCacheManagersTest {
   @Override
   public Object[] factory() {
      return new Object[] {
         new StateTransferOverwritingValueTest().cacheMode(CacheMode.DIST_SYNC).transactional(false),
         new StateTransferOverwritingValueTest().cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.OPTIMISTIC),
         new StateTransferOverwritingValueTest().cacheMode(CacheMode.DIST_SYNC).transactional(true).lockingMode(LockingMode.PESSIMISTIC),
         new StateTransferOverwritingValueTest().cacheMode(CacheMode.SCATTERED_SYNC).transactional(false),
      };
   }

   {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getConfigurationBuilder();

      addClusterEnabledCacheManager(c);
      waitForClusterToForm();
   }

   @Override
   protected void amendCacheManagerBeforeStart(EmbeddedCacheManager cm) {
      NoOpGlobalConfigurationManager.amendCacheManager(cm);
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(cacheMode);
      c.transaction().transactionMode(transactionMode());
      if (lockingMode != null) {
         c.transaction().lockingMode(lockingMode);
      }
      c.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
      return c;
   }

   public void testBackupOwnerJoiningDuringPut() throws Exception {
      doTest(TestWriteOperation.PUT_CREATE);
   }

   public void testBackupOwnerJoiningDuringPutFunctional() throws Exception {
      doTest(TestWriteOperation.PUT_CREATE_FUNCTIONAL);
   }

   public void testBackupOwnerJoiningDuringPutOverwrite() throws Exception {
      doTest(TestWriteOperation.PUT_OVERWRITE);
   }

   public void testBackupOwnerJoiningDuringPutOverwriteFunctional() throws Exception {
      doTest(TestWriteOperation.PUT_OVERWRITE_FUNCTIONAL);
   }

   public void testBackupOwnerJoiningDuringPutIfAbsent() throws Exception {
      doTest(TestWriteOperation.PUT_IF_ABSENT);
   }

   public void testBackupOwnerJoiningDuringPutIfAbsentFunctional() throws Exception {
      doTest(TestWriteOperation.PUT_IF_ABSENT_FUNCTIONAL);
   }

   public void testBackupOwnerJoiningDuringReplace() throws Exception {
      doTest(TestWriteOperation.REPLACE);
   }

   public void testBackupOwnerJoiningDuringReplaceFunctional() throws Exception {
      doTest(TestWriteOperation.REPLACE_FUNCTIONAL);
   }

   public void testBackupOwnerJoiningDuringReplaceWithPreviousValue() throws Exception {
      doTest(TestWriteOperation.REPLACE_EXACT);
   }

   public void testBackupOwnerJoiningDuringReplaceWithPreviousValueFunctional() throws Exception {
      doTest(TestWriteOperation.REPLACE_EXACT_FUNCTIONAL);
   }

   public void testBackupOwnerJoiningDuringRemove() throws Exception {
      doTest(TestWriteOperation.REMOVE);
   }

   public void testBackupOwnerJoiningDuringRemoveFunctional() throws Exception {
      doTest(TestWriteOperation.REMOVE_FUNCTIONAL);
   }

   public void testBackupOwnerJoiningDuringRemoveWithPreviousValue() throws Exception {
      doTest(TestWriteOperation.REMOVE_EXACT);
   }

   public void testBackupOwnerJoiningDuringRemoveWithPreviousValueFunctional() throws Exception {
      doTest(TestWriteOperation.REMOVE_EXACT_FUNCTIONAL);
   }

   private void doTest(final TestWriteOperation op) throws Exception {
      // Test scenario:
      // cache0 is the only member in the cluster, cache1 joins
      // Key k is in the cache, and is transferred to cache1
      // A user operation/tx also modifies key k
      // The user tx must update k even if it doesn't find key k in the data container.
      final AdvancedCache<Object, Object> cache0 = advancedCache(0);
      final String key = "key";

      // Prepare for replace/remove: put a previous value in cache0
      final Object previousValue = op.getPreviousValue();
      if (previousValue != null) {
         cache0.put(key, previousValue);
         assertEquals(previousValue, cache0.get(key));
         log.tracef("Previous value inserted: %s = %s", key, previousValue);
      }

      int preJoinTopologyId = cache0.getComponentRegistry().getStateTransferManager().getCacheTopology().getTopologyId();

      // Block any state response commands on cache0
      CheckPoint checkPoint = new CheckPoint();
      ControlledRpcManager blockingRpcManager0 = replaceRpcManager(cache0);
      blockingRpcManager0.excludeCommands(WriteCommand.class, BackupWriteCommand.class,
                                          RevokeBiasCommand.class, InvalidateVersionsCommand.class,
                                          AbstractTransactionBoundaryCommand.class,
                                          TxCompletionNotificationCommand.class);

      int rebalanceTopologyId = preJoinTopologyId + 1;
      // Block the rebalance confirmation on cache0
      blockRebalanceConfirmation(manager(0), checkPoint, rebalanceTopologyId);

      // Start the joiner
      log.tracef("Starting the cache on the joiner");
      ConfigurationBuilder c = getConfigurationBuilder();
      c.clustering().stateTransfer().awaitInitialTransfer(false);
      addClusterEnabledCacheManager(c);

      final AdvancedCache<Object,Object> cache1 = advancedCache(1);

      // Wait for the write CH to contain the joiner everywhere
      eventually(() -> cache0.getRpcManager().getMembers().size() == 2 &&
            cache1.getRpcManager().getMembers().size() == 2);

      // Every PutKeyValueCommand will be blocked before committing the entry on cache1
      CyclicBarrier beforeCommitCache1Barrier = new CyclicBarrier(2);
      // The implementation is free to change the command for backup or transactional modification
      BlockingInterceptor blockingInterceptor1 = new BlockingInterceptor<>(beforeCommitCache1Barrier,
            true, false, WriteCommand.class::isInstance);

      Class<? extends AsyncInterceptor> ewi = cache1.getAsyncInterceptorChain().getInterceptors().stream()
            .filter(i -> i instanceof EntryWrappingInterceptor).findFirst().orElseThrow(IllegalStateException::new).getClass();
      assertTrue(cache1.getAsyncInterceptorChain().addInterceptorAfter(blockingInterceptor1, ewi));

      // Wait for cache0 to collect the state to send to cache1 (including our previous value).
      ControlledRpcManager.BlockedRequest blockedStateResponse =
         blockingRpcManager0.expectCommand(StateResponseCommand.class);

      // Put/Replace/Remove from cache0 with cache0 as primary owner, cache1 will become a backup owner for the retry
      // The put command will be blocked on cache1 just before committing the entry.
      Future<Object> future = fork(() -> op.perform(cache0, key));

      // Wait for the entry to be wrapped on cache1
      beforeCommitCache1Barrier.await(10, TimeUnit.SECONDS);
      // Stop blocking, otherwise we'll block the state transfer put commands as well
      blockingInterceptor1.suspend(true);

      // Allow the state to be applied on cache1 (writing the old value for our entry)
      blockedStateResponse.send().receiveAll();
      if (cacheMode.isScattered()) {
         blockingRpcManager0.expectCommand(StateResponseCommand.class).send().receiveAll();
      }

      // Wait for cache1 to finish applying the state, but don't allow the rebalance confirmation to be processed.
      // (It would change the topology and it would trigger a retry for the command.)
      checkPoint.awaitStrict("pre_rebalance_confirmation_" + rebalanceTopologyId + "_from_" + address(1), 10, SECONDS);

      // Now allow the command to commit on cache1
      beforeCommitCache1Barrier.await(10, TimeUnit.SECONDS);

      // Wait for the command to finish and check that it didn't fail
      Object result = future.get(10, TimeUnit.SECONDS);
      assertEquals(op.getReturnValue(), result);
      log.tracef("%s operation is done", op);

      // Allow the rebalance confirmation to proceed and wait for the topology to change everywhere
      checkPoint.trigger("resume_rebalance_confirmation_" + rebalanceTopologyId + "_from_" + address(0));
      checkPoint.trigger("resume_rebalance_confirmation_" + rebalanceTopologyId + "_from_" + address(1));
      TestingUtil.waitForNoRebalance(cache0, cache1);

      // Check the value on all the nodes
      assertEquals(op.getValue(), cache0.get(key));
      assertEquals(op.getValue(), cache1.get(key));

      blockingRpcManager0.stopBlocking();
   }

   private void blockRebalanceConfirmation(final EmbeddedCacheManager manager, final CheckPoint checkPoint, int rebalanceTopologyId)
         throws Exception {
      ClusterTopologyManager ctm = TestingUtil.extractGlobalComponent(manager, ClusterTopologyManager.class);
      ClusterTopologyManager spyManager = spy(ctm);
      doAnswer(invocation -> {
         Object[] arguments = invocation.getArguments();
         Address source = (Address) arguments[1];
         int topologyId = (Integer) arguments[2];
         if (topologyId == rebalanceTopologyId) {
            checkPoint.trigger("pre_rebalance_confirmation_" + topologyId + "_from_" + source);
            checkPoint.awaitStrict("resume_rebalance_confirmation_" + topologyId + "_from_" + source, 10, SECONDS);
         }
         return invocation.callRealMethod();
      }).when(spyManager).handleRebalancePhaseConfirm(anyString(), any(Address.class), anyInt(), isNull(), anyInt());
      TestingUtil.replaceComponent(manager, ClusterTopologyManager.class, spyManager, true);
   }
}
