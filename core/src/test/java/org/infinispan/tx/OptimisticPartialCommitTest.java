package org.infinispan.tx;

import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnGlobalComponentMethod;
import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnInterceptor;
import static org.infinispan.test.concurrent.StateSequencerUtil.matchCommand;
import static org.infinispan.test.concurrent.StateSequencerUtil.matchMethodCall;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.statetransfer.StateTransferInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.concurrent.InvocationMatcher;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.configuration.cache.IsolationLevel;
import org.testng.annotations.Test;

/**
 * Test that the modifications of a transaction that were not committed on a node because it didn't own all the keys
 * are still applied after the node becomes an owner for all of them.
 *
 * @author Dan Berindei
 */
@CleanupAfterMethod
@Test(groups = "functional", testName = "tx.OptimisticPartialCommitTest")
public class OptimisticPartialCommitTest extends MultipleCacheManagersTest {

   private ControlledConsistentHashFactory controlledCHFactory;

   @Override
   protected void createCacheManagers() throws Throwable {
      controlledCHFactory = new ControlledConsistentHashFactory.Default(new int[][]{{1, 2}, {2, 3}});
      ConfigurationBuilder configuration = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      configuration.clustering().cacheMode(CacheMode.DIST_SYNC);
      configuration.clustering().hash().numSegments(2).numOwners(2).consistentHashFactory(controlledCHFactory);
      configuration.transaction().lockingMode(LockingMode.OPTIMISTIC)
            .locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      createCluster(ControlledConsistentHashFactory.SCI.INSTANCE, configuration, 4);
      waitForClusterToForm();
   }

   public void testNonOwnerBecomesOwnerDuringCommit() throws Exception {
      final Object k1 = new MagicKey("k1", cache(1), cache(2));
      final Object k2 = new MagicKey("k2", cache(2), cache(3));

      cache(0).put(k1, "v1_0");
      cache(0).put(k2, "v2_0");

      // commit on cache 0 -> send commit to 1, 2, 3 -> block commit on 2 -> wait for the commit on 1 to finish
      // -> kill 3 -> rebalance -> 1 applies state from 2 -> 2 resends commit to 1 -> 1 commits again (including k2)
      // Without the fix, the second commit is ignored and k2 is not updated
      StateSequencer ss = new StateSequencer();
      ss.logicalThread("main", "after_commit_on_1", "before_kill_3",
            "after_state_applied_on_1", "before_commit_on_2", "after_commit_on_2");

      advanceOnInterceptor(ss, cache(1), StateTransferInterceptor.class,
            matchCommand(VersionedCommitCommand.class).matchCount(0).build())
            .after("after_commit_on_1");
      advanceOnInterceptor(ss, cache(2), StateTransferInterceptor.class,
            matchCommand(VersionedCommitCommand.class).matchCount(0).build())
            .before("before_commit_on_2").after("after_commit_on_2");

      InvocationMatcher stateAppliedOn0Matcher = matchMethodCall("handleRebalancePhaseConfirm")
            .withParam(1, address(1)).build();
      advanceOnGlobalComponentMethod(ss, manager(0), ClusterTopologyManager.class, stateAppliedOn0Matcher)
            .after("after_state_applied_on_1");

      Future<Object> txFuture = fork(() -> {
         tm(0).begin();
         try {
            cache(0).put(k1, "v1_1");
            cache(0).put(k2, "v2_1");
         } finally {
            tm(0).commit();
         }
         return null;
      });

      ss.advance("before_kill_3");
      controlledCHFactory.setOwnerIndexes(new int[][]{{1, 2}, {2, 1}});
      manager(3).stop();
      cacheManagers.remove(3);

      txFuture.get(30, TimeUnit.SECONDS);

      assertEquals("v1_1", cache(1).get(k1));
      assertEquals("v2_1", cache(1).get(k2));

      assertEquals("v1_1", cache(2).get(k1));
      assertEquals("v2_1", cache(2).get(k2));
   }

   public void testOriginatorBecomesOwnerDuringCommit() throws Exception {
      final Object k1 = new MagicKey("k1", cache(1), cache(2));
      final Object k2 = new MagicKey("k2", cache(2), cache(3));

      cache(1).put(k1, "v1_0");
      cache(1).put(k2, "v2_0");

      // commit on cache 1 -> send commit to 2 and 3 -> kill 3
      // -> rebalance -> 1 receives state from 2 -> 2 doesn't resend commit to 1 -> 1 finishes commit
      // Cache 1 wrapped k2 before the prepare, so it will update it during commit even without repeating the commit
      StateSequencer ss = new StateSequencer();
      ss.logicalThread("main", "before_kill_3",
            "after_state_applied_on_1", "before_commit_on_2", "after_commit_on_2", "after_commit_on_1");

      // avoids blockhound exception in the next method
      TransactionTable transactionTable = transactionTable(1);

      advanceOnInterceptor(ss, cache(1), StateTransferInterceptor.class,
            command -> {
               if (!(command instanceof VersionedCommitCommand))
                  return false;
               GlobalTransaction gtx = ((VersionedCommitCommand) command).getGlobalTransaction();
               LocalTransaction tx = transactionTable.getLocalTransaction(gtx);
               return tx.getStateTransferFlag() == null;
            }).after("after_commit_on_1");
      advanceOnInterceptor(ss, cache(2), StateTransferInterceptor.class,
            matchCommand(VersionedCommitCommand.class).matchCount(0).build())
            .before("before_commit_on_2").after("after_commit_on_2");

      InvocationMatcher stateAppliedOn0Matcher = matchMethodCall("handleRebalancePhaseConfirm")
            .withParam(1, address(1)).build();
      advanceOnGlobalComponentMethod(ss, manager(0), ClusterTopologyManager.class, stateAppliedOn0Matcher)
            .after("after_state_applied_on_1");

      Future<Object> txFuture = fork(() -> {
         tm(0).begin();
         try {
            cache(1).put(k1, "v1_1");
            cache(1).put(k2, "v2_1");
         } finally {
            tm(0).commit();
         }
         return null;
      });

      ss.advance("before_kill_3");
      controlledCHFactory.setOwnerIndexes(new int[][]{{1, 2}, {2, 1}});
      manager(3).stop();
      cacheManagers.remove(3);

      txFuture.get(30, TimeUnit.SECONDS);

      assertEquals("v1_1", cache(1).get(k1));
      assertEquals("v2_1", cache(1).get(k2));

      assertEquals("v1_1", cache(2).get(k1));
      assertEquals("v2_1", cache(2).get(k2));
   }
}
