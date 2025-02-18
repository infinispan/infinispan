package org.infinispan.distribution.rehash;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.commands.triangle.BackupWriteCommand;
import org.infinispan.commands.write.BackupAckCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ClearCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.globalstate.NoOpGlobalConfigurationManager;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CacheEntryDelegator;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.ClusteringDependentLogicDelegator;
import org.infinispan.test.op.TestWriteOperation;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.ControlledRpcManager;
import org.mockito.AdditionalAnswers;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

/**
 * Tests that state transfer can't overwrite a value written by a command during state transfer.
 * See https://issues.jboss.org/browse/ISPN-3443
 *
 * @author Dan Berindei
 * @since 6.0
 */
@Test(groups = "functional", testName = "distribution.rehash.NonTxStateTransferOverwritingValue2Test")
public class NonTxStateTransferOverwritingValue2Test extends MultipleCacheManagersTest {

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
      c.clustering().cacheMode(CacheMode.DIST_SYNC);
      c.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      return c;
   }

   public void testBackupOwnerJoiningDuringPutOverwrite() throws Exception {
      // Need a previous value for this test, so we can't test PUT_CREATE
      doTest(TestWriteOperation.PUT_OVERWRITE);
   }

   public void testBackupOwnerJoiningDuringReplace() throws Exception {
      doTest(TestWriteOperation.REPLACE);
   }

   public void testBackupOwnerJoiningDuringReplaceWithPreviousValue() throws Exception {
      doTest(TestWriteOperation.REPLACE_EXACT);
   }

   public void testBackupOwnerJoiningDuringRemove() throws Exception {
      doTest(TestWriteOperation.REMOVE);
   }

   public void testBackupOwnerJoiningDuringRemoveWithPreviousValue() throws Exception {
      doTest(TestWriteOperation.REMOVE_EXACT);
   }

   private void doTest(final TestWriteOperation op) throws Exception {
      // Test scenario:
      // cache0 is the only member in the cluster, cache1 joins
      // Key k is in the cache, and is transferred to cache1
      // A user operation/tx also modifies key k
      // Even if both state transfer and the user tx try to commit the entry for k concurrently,
      // the value of k at the end should be the one set by the user tx.
      final AdvancedCache<Object, Object> cache0 = advancedCache(0);
      final String key = "key";

      // Prepare for replace/remove: put a previous value in cache0
      final Object previousValue = op.getPreviousValue();
      if (previousValue != null) {
         cache0.put(key, previousValue);
         assertEquals(previousValue, cache0.get(key));
         log.tracef("Previous value inserted: %s = %s", key, previousValue);
      }

      int preJoinTopologyId = cache0.getDistributionManager().getCacheTopology().getTopologyId();

      // Block any state response commands on cache0
      // So that we can install the spy ClusteringDependentLogic on cache1 before state transfer is applied
      final CheckPoint checkPoint = new CheckPoint();
      ControlledRpcManager blockingRpcManager0 = ControlledRpcManager.replaceRpcManager(cache0);
      blockingRpcManager0.excludeCommands(BackupWriteCommand.class, BackupAckCommand.class);

      // Block the rebalance confirmation on coordinator (to avoid the retrying of commands)
      blockRebalanceConfirmation(manager(0), checkPoint, preJoinTopologyId + 1);

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
      blockEntryCommit(checkPoint, cache1);

      // Wait for cache0 to collect the state to send to cache1 (including our previous value).
      ControlledRpcManager.BlockedRequest blockedStateResponse =
         blockingRpcManager0.expectCommand(StateResponseCommand.class);
      // Allow the state to be applied on cache1 (writing the old value for our entry)
      ControlledRpcManager.SentRequest sentStateResponse = blockedStateResponse.send();

      // Wait for state transfer tx/operation to call commitEntry on cache1 and block
      checkPoint.awaitStrict("pre_commit_entry_" + key + "_from_" + null, 5, SECONDS);

      // Put/Replace/Remove from cache0 with cache0 as primary owner, cache1 as backup owner
      // The put command will be blocked on cache1 just before committing the entry.
      Future<Object> future = fork(() -> op.perform(cache0, key));

      // Check that the user write is blocked by the state transfer write
      boolean blocked = checkPoint.peek(1, SECONDS, "pre_commit_entry_" + key + "_from_" + address(0)) == null;
      assertTrue(blocked);

      // Allow state transfer to commit
      checkPoint.trigger("resume_commit_entry_" + key + "_from_" + null);

      // Check that the user operation can now commit the entry
      checkPoint.awaitStrict("pre_commit_entry_" + key + "_from_" + address(0), 5, SECONDS);

      // Allow the user put to commit
      checkPoint.trigger("resume_commit_entry_" + key + "_from_" + address(0));

      // Wait for both state transfer and the command to commit
      checkPoint.awaitStrict("post_commit_entry_" + key + "_from_" + null, 10, SECONDS);
      checkPoint.awaitStrict("post_commit_entry_" + key + "_from_" + address(0), 10, SECONDS);

      // Wait for the command to finish and check that it didn't fail
      Object result = future.get(10, TimeUnit.SECONDS);
      assertEquals(op.getReturnValue(), result);
      log.tracef("%s operation is done", op);

      // Receive the response for the state response command (only after all commits have finished)
      sentStateResponse.receiveAll();

      // Allow the rebalance confirmation to proceed and wait for the topology to change everywhere
      int rebalanceTopologyId = preJoinTopologyId + 1;
      checkPoint.trigger("resume_rebalance_confirmation_" + rebalanceTopologyId + "_from_" + address(0));
      checkPoint.trigger("resume_rebalance_confirmation_" + rebalanceTopologyId + "_from_" + address(1));
      TestingUtil.waitForNoRebalance(cache0, cache1);

      // Check the value on all the nodes
      assertEquals(op.getValue(), cache0.get(key));
      assertEquals(op.getValue(), cache1.get(key));

      blockingRpcManager0.stopBlocking();
   }

   private void blockEntryCommit(final CheckPoint checkPoint, AdvancedCache<Object, Object> cache) {
      ClusteringDependentLogic cdl1 = TestingUtil.extractComponent(cache, ClusteringDependentLogic.class);
      ClusteringDependentLogic replaceCdl = new ClusteringDependentLogicDelegator(cdl1) {
         @Override
         public CompletionStage<Void> commitEntry(CacheEntry entry, FlagAffectedCommand command,
                                 InvocationContext ctx, Flag trackFlag, boolean l1Invalidation) {
            //skip for clear command!
            if (entry instanceof ClearCacheEntry) {
               return super.commitEntry(entry, command, ctx, trackFlag, l1Invalidation);
            }
            final Address source = ctx.getOrigin();
            CacheEntry newEntry = new CacheEntryDelegator(entry) {
               @Override
               public void commit(DataContainer container) {
                  checkPoint.trigger("pre_commit_entry_" + getKey() + "_from_" + source);
                  try {
                     checkPoint.awaitStrict("resume_commit_entry_" + getKey() + "_from_" + source, 10,
                           SECONDS);
                  } catch (InterruptedException | TimeoutException e) {
                     throw new RuntimeException(e);
                  }
                  super.commit(container);
                  checkPoint.trigger("post_commit_entry_" + getKey() + "_from_" + source);
               }
            };
            return super.commitEntry(newEntry, command, ctx, trackFlag, l1Invalidation);
         }
      };
      TestingUtil.replaceComponent(cache, ClusteringDependentLogic.class, replaceCdl, true);
   }

   private void blockRebalanceConfirmation(final EmbeddedCacheManager manager, final CheckPoint checkPoint, int rebalanceTopologyId)
         throws Exception {
      ClusterTopologyManager ctm = TestingUtil.extractGlobalComponent(manager, ClusterTopologyManager.class);
      Answer<?> forwardedAnswer = AdditionalAnswers.delegatesTo(ctm);
      ClusterTopologyManager mock = mock(ClusterTopologyManager.class, withSettings().defaultAnswer(forwardedAnswer));
      doAnswer(invocation -> {
         Object[] arguments = invocation.getArguments();
         Address source = (Address) arguments[1];
         int topologyId = (Integer) arguments[2];
         if (rebalanceTopologyId == topologyId) {
            checkPoint.trigger("pre_rebalance_confirmation_" + topologyId + "_from_" + source);
            return checkPoint.future("resume_rebalance_confirmation_" + topologyId + "_from_" + source, 10, SECONDS, testExecutor())
                  .thenCompose(__ -> Mocks.callAnotherAnswer(forwardedAnswer, invocation));
         }
         return forwardedAnswer.answer(invocation);
      }).when(mock).handleRebalancePhaseConfirm(anyString(), any(Address.class), anyInt(), isNull());
      TestingUtil.replaceComponent(manager, ClusterTopologyManager.class, mock, true);
   }
}
