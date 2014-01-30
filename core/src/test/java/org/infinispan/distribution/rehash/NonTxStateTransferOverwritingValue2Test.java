package org.infinispan.distribution.rehash;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.interceptors.locking.ClusteringDependentLogic;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.metadata.Metadata;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Tests that state transfer can't overwrite a value written by a command during state transfer.
 * See https://issues.jboss.org/browse/ISPN-3443
 *
 * @author Dan Berindei
 * @since 6.0
 */
@Test(groups = "functional", testName = "distribution.rehash.NonTxStateTransferOverwritingValue2Test")
public class NonTxStateTransferOverwritingValue2Test extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = BasicCacheContainer.DEFAULT_CACHE_NAME;

   {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getConfigurationBuilder();

      addClusterEnabledCacheManager(c);
      waitForClusterToForm();
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

      int preJoinTopologyId = cache0.getComponentRegistry().getStateTransferManager().getCacheTopology().getTopologyId();

      // Block any state response commands on cache0
      // So that we can install the spy ClusteringDependentLogic on cache1 before state transfer is applied
      final CheckPoint checkPoint = new CheckPoint();
      ControlledRpcManager blockingRpcManager0 = blockStateResponseCommand(cache0);

      // Block the rebalance confirmation on cache0 (to avoid the retrying of commands)
      blockRebalanceConfirmation(manager(0), checkPoint);

      // Start the joiner
      log.tracef("Starting the cache on the joiner");
      ConfigurationBuilder c = getConfigurationBuilder();
      c.clustering().stateTransfer().awaitInitialTransfer(false);
      addClusterEnabledCacheManager(c);

      final AdvancedCache<Object,Object> cache1 = advancedCache(1);

      // Wait for the write CH to contain the joiner everywhere
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cache0.getRpcManager().getMembers().size() == 2 &&
                  cache1.getRpcManager().getMembers().size() == 2;
         }
      });

      // Every PutKeyValueCommand will be blocked before committing the entry on cache1
      blockEntryCommit(checkPoint, cache1);

      // Wait for cache0 to collect the state to send to cache1 (including our previous value).
      blockingRpcManager0.waitForCommandToBlock();
      // Allow the state to be applied on cache1 (writing the old value for our entry)
      blockingRpcManager0.stopBlocking();

      // Wait for state transfer tx/operation to call commitEntry on cache1 and block
      checkPoint.awaitStrict("pre_commit_entry_" + key + "_from_" + null, 5, SECONDS);

      // Put/Replace/Remove from cache0 with cache0 as primary owner, cache1 as backup owner
      // The put command will be blocked on cache1 just before committing the entry.
      Future<Object> future = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            return op.perform(cache0, key);
         }
      });

      // Check that the user write is blocked by the state transfer write
      boolean blocked = checkPoint.peek(3, SECONDS, "pre_commit_entry_" + key + "_from_" + address(0)) == null;
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

      // Allow the rebalance confirmation to proceed and wait for the topology to change everywhere
      int rebalanceTopologyId = preJoinTopologyId + 1;
      checkPoint.trigger("resume_rebalance_confirmation_" + rebalanceTopologyId + "_from_" + address(0));
      checkPoint.trigger("resume_rebalance_confirmation_" + rebalanceTopologyId + "_from_" + address(1));
      TestingUtil.waitForRehashToComplete(cache0, cache1);

      // Check the value on all the nodes
      assertEquals(op.getValue(), cache0.get(key));
      assertEquals(op.getValue(), cache1.get(key));
   }

   private void blockEntryCommit(final CheckPoint checkPoint, AdvancedCache<Object, Object> cache) {
      ClusteringDependentLogic cdl1 = cache.getComponentRegistry().getComponent(ClusteringDependentLogic.class);
      ClusteringDependentLogic spyCdl1 = spy(cdl1);
      doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            Object[] arguments = invocation.getArguments();
            CacheEntry entry = (CacheEntry) arguments[0];
            Object key = entry.getKey();
            InvocationContext ctx = (InvocationContext) arguments[3];
            Address source = ctx.getOrigin();
            checkPoint.trigger("pre_commit_entry_" + key + "_from_" + source);
            checkPoint.awaitStrict("resume_commit_entry_" + key + "_from_" + source, 10, SECONDS);
            Object result = invocation.callRealMethod();
            checkPoint.trigger("post_commit_entry_" + key + "_from_" + source);
            return result;
         }
      }).when(spyCdl1).commitEntry(any(CacheEntry.class), any(Metadata.class), any(FlagAffectedCommand.class),
            any(InvocationContext.class));
      TestingUtil.replaceComponent(cache, ClusteringDependentLogic.class, spyCdl1, true);
   }

   private ControlledRpcManager blockStateResponseCommand(final Cache cache) throws InterruptedException {
      RpcManager rpcManager = TestingUtil.extractComponent(cache, RpcManager.class);
      ControlledRpcManager controlledRpcManager = new ControlledRpcManager(rpcManager);
      controlledRpcManager.blockBefore(StateResponseCommand.class);
      TestingUtil.replaceComponent(cache, RpcManager.class, controlledRpcManager, true);
      return controlledRpcManager;
   }

   private void blockRebalanceConfirmation(final EmbeddedCacheManager manager, final CheckPoint checkPoint)
         throws Exception {
      ClusterTopologyManager ctm = TestingUtil.extractGlobalComponent(manager, ClusterTopologyManager.class);
      ClusterTopologyManager spyManager = spy(ctm);
      doAnswer(new Answer<Object>() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            Object[] arguments = invocation.getArguments();
            Address source = (Address) arguments[1];
            int topologyId = (Integer) arguments[2];
            checkPoint.trigger("pre_rebalance_confirmation_" + topologyId + "_from_" + source);
            checkPoint.awaitStrict("resume_rebalance_confirmation_" + topologyId + "_from_" + source, 10, SECONDS);
            return invocation.callRealMethod();
         }
      }).when(spyManager).handleRebalanceCompleted(anyString(), any(Address.class), anyInt(), any(Throwable.class),
            anyInt());
      TestingUtil.replaceComponent(manager, ClusterTopologyManager.class, spyManager, true);
   }
}