package org.infinispan.distribution.rehash;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.commands.statetransfer.StateTransferGetTransactionsCommand;
import org.infinispan.commands.statetransfer.StateTransferStartCommand;
import org.infinispan.commands.triangle.BackupWriteCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.BackupAckCommand;
import org.infinispan.distribution.BaseDistFunctionalTest;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.distribution.MagicKey;
import org.infinispan.globalstate.NoOpGlobalConfigurationManager;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.statetransfer.StateTransferInterceptor;
import org.infinispan.test.Mocks;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.op.TestWriteOperation;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.util.ControlledRpcManager;
import org.mockito.AdditionalAnswers;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import jakarta.transaction.TransactionManager;

/**
 * Base class used to test various write commands interleaving with state transfer with a tx cache
 *
 * @author William Burns
 * @since 6.0
 */
@Test(groups = "functional")
public abstract class BaseTxStateTransferOverwriteTest extends BaseDistFunctionalTest<Object, Object> {
   public BaseTxStateTransferOverwriteTest() {
      INIT_CLUSTER_SIZE = 3;
      numOwners = 2;
      transactional = true;
      performRehashing = true;
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected boolean l1Enabled() {
      return cache(0, cacheName).getCacheConfiguration().clustering().l1().enabled();
   }

   @Override
   protected void amendCacheManagerBeforeStart(EmbeddedCacheManager cm) {
      NoOpGlobalConfigurationManager.amendCacheManager(cm);
   }

   /**
    * This command should return a class that extends {@link VisitableCommand} that should match
    * the command that will cause data to soon be placed into the data container.  Since this test is
    * transaction based the default value is to return a {@link PrepareCommand}, however other tests
    * can change this behavior if desired.
    */
   protected Class<? extends VisitableCommand> getVisitableCommand(TestWriteOperation op) {
      return PrepareCommand.class;
   }

   protected Callable<?> runWithTx(final TransactionManager tm, final Callable<?> callable) {
      return () -> TestingUtil.withTx(tm, callable);
   }

   @Test
   public void testStateTransferInBetweenPrepareCommitWithPut() throws Exception {
      doStateTransferInBetweenPrepareCommit(TestWriteOperation.PUT_OVERWRITE, true);
   }

   @Test
   public void testStateTransferInBetweenPrepareCommitMultipleEntryWithPut() throws Exception {
      doStateTransferInBetweenPrepareCommit(TestWriteOperation.PUT_OVERWRITE, false);
   }

   @Test
   public void testStateTransferInBetweenPrepareCommitWithPutCreate() throws Exception {
      doStateTransferInBetweenPrepareCommit(TestWriteOperation.PUT_CREATE, true);
   }

   @Test
   public void testStateTransferInBetweenPrepareCommitMultipleEntryWithPutCreate() throws Exception {
      doStateTransferInBetweenPrepareCommit(TestWriteOperation.PUT_CREATE, false);
   }

   @Test
   public void testStateTransferInBetweenPrepareCommitWithPutIfAbsent() throws Exception {
      doStateTransferInBetweenPrepareCommit(TestWriteOperation.PUT_IF_ABSENT, true);
   }

   @Test
   public void testStateTransferInBetweenPrepareCommitMultipleEntryWithPutIfAbsent() throws Exception {
      doStateTransferInBetweenPrepareCommit(TestWriteOperation.PUT_IF_ABSENT, false);
   }

   @Test
   public void testStateTransferInBetweenPrepareCommitWithRemoveExact() throws Exception {
      doStateTransferInBetweenPrepareCommit(TestWriteOperation.REMOVE_EXACT, true);
   }

   @Test
   public void testStateTransferInBetweenPrepareCommitMultipleEntryWithRemoveExact() throws Exception {
      doStateTransferInBetweenPrepareCommit(TestWriteOperation.REMOVE_EXACT, false);
   }

   @Test
   public void testStateTransferInBetweenPrepareCommitWithRemove() throws Exception {
      doStateTransferInBetweenPrepareCommit(TestWriteOperation.REMOVE, true);
   }

   @Test
   public void testStateTransferInBetweenPrepareCommitMultipleEntryWithRemove() throws Exception {
      doStateTransferInBetweenPrepareCommit(TestWriteOperation.REMOVE, false);
   }

   @Test
   public void testStateTransferInBetweenPrepareCommitWithReplace() throws Exception {
      doStateTransferInBetweenPrepareCommit(TestWriteOperation.REPLACE, true);
   }

   @Test
   public void testStateTransferInBetweenPrepareCommitMultipleEntryWithReplace() throws Exception {
      doStateTransferInBetweenPrepareCommit(TestWriteOperation.REPLACE, false);
   }

   @Test
   public void testStateTransferInBetweenPrepareCommitWithReplaceExact() throws Exception {
      doStateTransferInBetweenPrepareCommit(TestWriteOperation.REPLACE_EXACT, true);
   }

   @Test
   public void testStateTransferInBetweenPrepareCommitMultipleEntryWithReplaceExact() throws Exception {
      doStateTransferInBetweenPrepareCommit(TestWriteOperation.REPLACE_EXACT, false);
   }

   @Test
   public void testNonCoordinatorOwnerLeavingDuringPut() throws Exception {
      doTestWhereCommitOccursAfterStateTransferBeginsBeforeCompletion(TestWriteOperation.PUT_CREATE);
   }

   @Test
   public void testNonCoordinatorOwnerLeavingDuringPutIfAbsent() throws Exception {
      doTestWhereCommitOccursAfterStateTransferBeginsBeforeCompletion(TestWriteOperation.PUT_IF_ABSENT);
   }

   @Test
   public void testNonCoordinatorOwnerLeavingDuringPut2() throws Exception {
      doL1InvalidationOldTopologyComesAfterRebalance(TestWriteOperation.PUT_CREATE);
   }

   @Test
   public void testNonCoordinatorOwnerLeavingDuringPutOverwrite2() throws Exception {
      doL1InvalidationOldTopologyComesAfterRebalance(TestWriteOperation.PUT_OVERWRITE);
   }

   @Test
   public void testNonCoordinatorOwnerLeavingDuringPutIfAbsent2() throws Exception {
      doL1InvalidationOldTopologyComesAfterRebalance(TestWriteOperation.PUT_IF_ABSENT);
   }

   @Test
   public void testNonCoordinatorOwnerLeavingDuringReplace2() throws Exception {
      doL1InvalidationOldTopologyComesAfterRebalance(TestWriteOperation.REPLACE);
   }

   @Test
   public void testNonCoordinatorOwnerLeavingDuringReplaceWithPreviousValue2() throws Exception {
      doL1InvalidationOldTopologyComesAfterRebalance(TestWriteOperation.REPLACE_EXACT);
   }

   @Test
   public void testNonCoordinatorOwnerLeavingDuringRemove2() throws Exception {
      doL1InvalidationOldTopologyComesAfterRebalance(TestWriteOperation.REMOVE);
   }

   @Test
   public void testNonCoordinatorOwnerLeavingDuringRemoveWithPreviousValue2() throws Exception {
      doL1InvalidationOldTopologyComesAfterRebalance(TestWriteOperation.REMOVE_EXACT);
   }

   protected void doStateTransferInBetweenPrepareCommit(final TestWriteOperation op,
                                                      final boolean additionalValueOnNonOwner) throws Exception {
      // Test scenario:
      // cache0,1,2 are in the cluster, an owner leaves
      // Key k is in the cache, and is transferred to the non owner
      // A user operation also modifies key k causing an invalidation
      // on the non owner which is getting the state transfer
      final AdvancedCache<Object, Object> primaryOwnerCache = advancedCache(0, cacheName);
      final AdvancedCache<Object, Object> backupOwnerCache = advancedCache(1, cacheName);
      final AdvancedCache<Object, Object> nonOwnerCache = advancedCache(2, cacheName);
      final MagicKey key = new MagicKey(op + "-key", cache(0, cacheName), cache(1, cacheName));

      // Prepare for replace/remove: put a previous value in cache0
      final Object previousValue = op.getPreviousValue();
      if (previousValue != null) {
         primaryOwnerCache.put(key, previousValue);
         assertEquals(previousValue, primaryOwnerCache.get(key));
         log.tracef("Previous value inserted: %s = %s", key, previousValue);

         assertEquals(previousValue, nonOwnerCache.get(key));

         if (l1Enabled()) {
            assertIsInL1(nonOwnerCache, key);
         }
      }

      // Need to block after Prepare command was sent after it clears the StateTransferInterceptor
      final CyclicBarrier cyclicBarrier = new CyclicBarrier(2);

      try {
         TransactionManager tm = primaryOwnerCache.getTransactionManager();
         Future<?> future = fork(runWithTx(tm, () -> {
            if (additionalValueOnNonOwner) {
               MagicKey mk = new MagicKey("placeholder", nonOwnerCache);
               String value = "somevalue";
               primaryOwnerCache.put(mk, value);
               log.tracef("Adding additional value on nonOwner value inserted: %s = %s", mk, value);
            }
            extractInterceptorChain(primaryOwnerCache)
                  .addInterceptorBefore(new BlockingInterceptor<>(cyclicBarrier, getVisitableCommand(op), true, false),
                                        StateTransferInterceptor.class);
            return op.perform(primaryOwnerCache, key);
         }));

         cyclicBarrier.await(10, SECONDS);

         // After the barrier  has been hit remove the interceptor, since we can just wake it up through the barrier,
         // this way the state transfer won't be blocked if the normal put occurs before it.
         removeAllBlockingInterceptorsFromCache(primaryOwnerCache);

         // Block the rebalance confirmation on nonOwnerCache
         CheckPoint checkPoint = new CheckPoint();
         log.trace("Adding proxy to state transfer");
         waitUntilStateBeingTransferred(nonOwnerCache, checkPoint);

         backupOwnerCache.getCacheManager().stop();

         // Wait for non owner to just about get state
         checkPoint.awaitStrict("pre_state_apply_invoked_for_" + nonOwnerCache, 10, SECONDS);

         // let prepare complete and thus commit command invalidating on nonOwner
         cyclicBarrier.await(10, SECONDS);

         assertEquals(op.getReturnValue(), future.get(10, SECONDS));

         // let state transfer go
         checkPoint.trigger("pre_state_apply_release_for_" + nonOwnerCache);
         TestingUtil.waitForNoRebalance(primaryOwnerCache, nonOwnerCache);

         switch (op) {
            case REMOVE:
            case REMOVE_EXACT:
               break;
            default:
               assertIsInContainerImmortal(primaryOwnerCache, key);
               assertIsInContainerImmortal(nonOwnerCache, key);
               break;
         }

         // Check the value to make sure data container contains correct value
         assertEquals(op.getValue(), primaryOwnerCache.get(key));
         assertEquals(op.getValue(), nonOwnerCache.get(key));
      } finally {
          removeAllBlockingInterceptorsFromCache(primaryOwnerCache);
      }
   }

   /**
    * When L1 is enabled this test should not be ran when a previous value is present as it will cause timeouts.  Due
    * to how locking works with L1 this cannot occur when the previous value exists.
    */
   protected void doTestWhereCommitOccursAfterStateTransferBeginsBeforeCompletion(final TestWriteOperation op) throws Exception {
      if (l1Enabled() && op.getPreviousValue() != null) {
         fail("This test cannot be ran with L1 when a previous value is set");
      }
      // Test scenario:
      // cache0,1,2 are in the cluster, an owner leaves
      // Key k is in the cache, and is transferred to the non owner
      // A user operation also modifies key k causing an invalidation
      // on the non owner which is getting the state transfer
      final AdvancedCache<Object, Object> primaryOwnerCache = cache(0, cacheName).getAdvancedCache();
      final AdvancedCache<Object, Object> backupOwnerCache = cache(1, cacheName).getAdvancedCache();
      final AdvancedCache<Object, Object> nonOwnerCache = cache(2, cacheName).getAdvancedCache();
      final MagicKey key = new MagicKey(primaryOwnerCache, backupOwnerCache);

      // Prepare for replace/remove: put a previous value in cache0
      final Object previousValue = op.getPreviousValue();
      if (previousValue != null) {
         primaryOwnerCache.put(key, previousValue);
         assertEquals(previousValue, primaryOwnerCache.get(key));
         log.tracef("Previous value inserted: %s = %s", key, previousValue);

         assertEquals(previousValue, nonOwnerCache.get(key));

         if (l1Enabled()) {
            assertIsInL1(nonOwnerCache, key);
         }
      }

      int preJoinTopologyId = primaryOwnerCache.getDistributionManager().getCacheTopology().getTopologyId();

      // Block any state response commands on cache0
      CheckPoint checkPoint = new CheckPoint();
      ControlledRpcManager blockingRpcManager0 = ControlledRpcManager.replaceRpcManager(primaryOwnerCache);
      ControlledRpcManager blockingRpcManager2 = ControlledRpcManager.replaceRpcManager(nonOwnerCache);
      // The execution of the write/prepare/commit commands is controlled with the BlockingInterceptor
      blockingRpcManager0.excludeCommands(BackupWriteCommand.class, PrepareCommand.class, CommitCommand.class,
                                          TxCompletionNotificationCommand.class
      );
      blockingRpcManager2.excludeCommands(BackupAckCommand.class);

      // Block the rebalance confirmation on cache0
      int rebalanceTopologyId = preJoinTopologyId + 2;
      blockRebalanceConfirmation(primaryOwnerCache.getCacheManager(), checkPoint, rebalanceTopologyId);

      assertEquals(primaryOwnerCache.getCacheManager().getCoordinator(),
                   primaryOwnerCache.getCacheManager().getAddress());

      // Remove the leaver
      log.trace("Stopping the cache");
      backupOwnerCache.getCacheManager().stop();

      // Wait for the write CH to contain the joiner everywhere
      eventuallyEquals(2, () -> primaryOwnerCache.getRpcManager().getMembers().size());
      eventuallyEquals(2, () -> nonOwnerCache.getRpcManager().getMembers().size());

      assertEquals(primaryOwnerCache.getCacheManager().getCoordinator(),
                   primaryOwnerCache.getCacheManager().getAddress());

      // Wait for both nodes to start state transfer
      if (transactional) {
         blockingRpcManager0.expectCommand(StateTransferGetTransactionsCommand.class).send().receiveAll();
         blockingRpcManager2.expectCommand(StateTransferGetTransactionsCommand.class).send().receiveAll();
      }
      ControlledRpcManager.BlockedRequest<StateTransferStartCommand> blockedStateRequest0 =
            blockingRpcManager0.expectCommand(StateTransferStartCommand.class);
      ControlledRpcManager.BlockedRequest<StateTransferStartCommand> blockedStateRequest2 =
            blockingRpcManager2.expectCommand(StateTransferStartCommand.class);

      // Unblock the state request from node 2
      // Don't wait for response, because node 2 might be sending the first state response on the request thread
      blockedStateRequest2.send().receiveAllAsync();
      // Wait for cache0 to collect the state to send to node 2 (including our previous value).
      ControlledRpcManager.BlockedRequest<StateResponseCommand> blockedStateResponse0 =
            blockingRpcManager0.expectCommand(StateResponseCommand.class);

      // Every PutKeyValueCommand will be blocked before committing the entry on cache1
      CyclicBarrier beforeCommitCache1Barrier = new CyclicBarrier(2);
      BlockingInterceptor<?> blockingInterceptor1 =
            new BlockingInterceptor<>(beforeCommitCache1Barrier, op.getCommandClass(), true, false);
      extractInterceptorChain(nonOwnerCache)
            .addInterceptorAfter(blockingInterceptor1, EntryWrappingInterceptor.class);

      // Put/Replace/Remove from cache0 with cache0 as primary owner, cache1 will become a backup owner for the retry
      // The put command will be blocked on cache1 just before committing the entry.
      Future<Object> future = fork(() -> op.perform(primaryOwnerCache, key));

      // Wait for the entry to be wrapped on node 2
      // The replicated command could be either a non-tx BackupWriteCommand or a PrepareCommand
      beforeCommitCache1Barrier.await(10, TimeUnit.SECONDS);

      // Remove the interceptor so we don't mess up any other state transfer puts
      removeAllBlockingInterceptorsFromCache(nonOwnerCache);

      // Allow the state to be applied on cache1 (writing the old value for our entry)
      blockedStateResponse0.send().receiveAll();

      // Wait for second in line to finish applying the state, but don't allow the rebalance confirmation to be processed.
      // (It would change the topology and it would trigger a retry for the command.)
      // Don't wait for response, because node 2 might be sending the first state response on the request thread
      blockedStateRequest0.send().receiveAllAsync();
      blockingRpcManager2.expectCommand(StateResponseCommand.class).send().receiveAll();
      checkPoint.awaitStrict("pre_rebalance_confirmation_" + rebalanceTopologyId + "_from_" +
                                   primaryOwnerCache.getCacheManager().getAddress(), 10, SECONDS);

      // Now allow the command to commit on cache1
      beforeCommitCache1Barrier.await(10, TimeUnit.SECONDS);

      // Wait for the command to finish and check that it didn't fail
      Object result = future.get(10, TimeUnit.SECONDS);
      assertEquals(op.getReturnValue(), result);
      log.tracef("%s operation is done", op);

      // Allow the rebalance confirmation to proceed and wait for the topology to change everywhere
      checkPoint.trigger("resume_rebalance_confirmation_" + rebalanceTopologyId + "_from_" + primaryOwnerCache.getCacheManager().getAddress());
      checkPoint.trigger("resume_rebalance_confirmation_" + rebalanceTopologyId + "_from_" + nonOwnerCache.getCacheManager().getAddress());
      TestingUtil.waitForNoRebalance(primaryOwnerCache, nonOwnerCache);

      switch (op) {
         case REMOVE:
         case REMOVE_EXACT:
            break;
         default:
            assertIsInContainerImmortal(primaryOwnerCache, key);
            assertIsInContainerImmortal(nonOwnerCache, key);
            break;
      }

      // Check the value to make sure data container contains correct value
      assertEquals(op.getValue(), primaryOwnerCache.get(key));
      assertEquals(op.getValue(), nonOwnerCache.get(key));
   }

   private void doL1InvalidationOldTopologyComesAfterRebalance(final TestWriteOperation op) throws Exception {
      // Test scenario:
      // cache0,1,2 are in the cluster, an owner leaves
      // Key k is in the cache, and is transferred to the non owner
      // A user operation also modifies key k causing an invalidation
      // on the non owner which is getting the state transfer
      final AdvancedCache<Object, Object> primaryOwnerCache = advancedCache(0, cacheName);
      final AdvancedCache<Object, Object> backupOwnerCache = advancedCache(1, cacheName);
      final AdvancedCache<Object, Object> nonOwnerCache = advancedCache(2, cacheName);
      final MagicKey key = new MagicKey(op + "-key", cache(0, cacheName), cache(1, cacheName));

      // Prepare for replace/remove: put a previous value in cache0
      final Object previousValue = op.getPreviousValue();
      if (previousValue != null) {
         primaryOwnerCache.put(key, previousValue);
         assertEquals(previousValue, primaryOwnerCache.get(key));
         log.tracef("Previous value inserted: %s = %s", key, previousValue);

         assertEquals(previousValue, nonOwnerCache.get(key));

         if (l1Enabled()) {
            assertIsInL1(nonOwnerCache, key);
         }
      }

      // Block on the interceptor right after ST which should now have the soon to be old topology id
      CyclicBarrier beforeCommitCache1Barrier = new CyclicBarrier(2);
      BlockingInterceptor<?> blockingInterceptor1 =
            new BlockingInterceptor<>(beforeCommitCache1Barrier, getVisitableCommand(op), false, false);
      extractInterceptorChain(primaryOwnerCache)
            .addInterceptorAfter(blockingInterceptor1, StateTransferInterceptor.class);

      // Put/Replace/Remove from primary owner.  This will block before it is committing on remote nodes
      Future<Object> future = fork(() -> {
         try {
            return op.perform(primaryOwnerCache, key);
         } finally {
            log.tracef("%s operation is done", op);
         }
      });

      beforeCommitCache1Barrier.await(10, SECONDS);

      // Remove blocking interceptor now since we have blocked
      removeAllBlockingInterceptorsFromCache(primaryOwnerCache);

      // Remove the leaver
      log.tracef("Stopping the cache");
      backupOwnerCache.getCacheManager().stop();

      // Wait for the write CH to contain the joiner everywhere
      eventually(() -> primaryOwnerCache.getRpcManager().getMembers().size() == 2 &&
            nonOwnerCache.getRpcManager().getMembers().size() == 2);

      TestingUtil.waitForNoRebalance(primaryOwnerCache, nonOwnerCache);

      // Now let the update go through
      beforeCommitCache1Barrier.await(10, SECONDS);

      // Run the update now that we are in the middle of a rebalance
      assertEquals(op.getReturnValue(), future.get(10, SECONDS));
      log.tracef("%s operation is done", op);

      switch (op) {
         case REMOVE:
         case REMOVE_EXACT:
            break;
         default:
            assertIsInContainerImmortal(primaryOwnerCache, key);
            assertIsInContainerImmortal(nonOwnerCache, key);
            break;
      }

      // Check the value to make sure data container contains correct value
      assertEquals(op.getValue(), primaryOwnerCache.get(key));
      assertEquals(op.getValue(), nonOwnerCache.get(key));
   }

   private void blockRebalanceConfirmation(final EmbeddedCacheManager manager, final CheckPoint checkPoint, int rebalanceTopologyId)
         throws Exception {
      ClusterTopologyManager ctm = TestingUtil.extractGlobalComponent(manager, ClusterTopologyManager.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(ctm);
      ClusterTopologyManager mockManager = mock(ClusterTopologyManager.class,
                                                withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(invocation -> {
         Object[] arguments = invocation.getArguments();
         Address source = (Address) arguments[1];
         int topologyId = (Integer) arguments[2];
         if (topologyId == rebalanceTopologyId) {
            checkPoint.trigger("pre_rebalance_confirmation_" + topologyId + "_from_" + source);
            return checkPoint.future("resume_rebalance_confirmation_" + topologyId + "_from_" + source,
                                     20, SECONDS, testExecutor())
                             .thenCompose(ignored -> Mocks.callAnotherAnswer(forwardedAnswer, invocation));
         }
         return forwardedAnswer.answer(invocation);
      }).when(mockManager).handleRebalancePhaseConfirm(anyString(), any(), anyInt(), isNull());
      TestingUtil.replaceComponent(manager, ClusterTopologyManager.class, mockManager, true);
   }

   protected void waitUntilStateBeingTransferred(final Cache<?, ?> cache, final CheckPoint checkPoint) {
      StateConsumer sc = TestingUtil.extractComponent(cache, StateConsumer.class);
      final Answer<Object> forwardedAnswer = AdditionalAnswers.delegatesTo(sc);
      StateConsumer mockConsumer = mock(StateConsumer.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(invocation -> {
         // Wait for main thread to sync up
         checkPoint.trigger("pre_state_apply_invoked_for_" + cache);
         // Now wait until main thread lets us through
         checkPoint.awaitStrict("pre_state_apply_release_for_" + cache, 20, TimeUnit.SECONDS);

         return forwardedAnswer.answer(invocation);
      }).when(mockConsumer).applyState(any(Address.class), anyInt(), anyCollection());
      TestingUtil.replaceComponent(cache, StateConsumer.class, mockConsumer, true);
   }
}
