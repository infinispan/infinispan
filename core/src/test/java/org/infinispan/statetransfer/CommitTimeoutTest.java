package org.infinispan.statetransfer;

import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnInterceptor;
import static org.infinispan.test.concurrent.StateSequencerUtil.matchCommand;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import jakarta.transaction.RollbackException;

import org.infinispan.Cache;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

/**
 * Test that a commit command that has timed out on a backup owner cannot write entries after the locks have been
 * released on the primary owner.
 */
@Test(groups = "functional", testName = "statetransfer.CommitTimeoutTest")
@CleanupAfterMethod
public class CommitTimeoutTest extends MultipleCacheManagersTest {

   private static final String TEST_KEY = "key";
   private static final String TX1_VALUE = "value1";
   private static final String TX2_VALUE = "value2";

   @Override
   protected void createCacheManagers() throws Throwable {
      ControlledConsistentHashFactory consistentHashFactory = new ControlledConsistentHashFactory.Default(1, 2);

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      builder.clustering().remoteTimeout(2000);
      builder.clustering().hash().numSegments(1).consistentHashFactory(consistentHashFactory);
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);

      createCluster(ControlledConsistentHashFactory.SCI.INSTANCE, builder, 3);
      waitForClusterToForm();
   }

   public void testCommitDoesntWriteAfterRollback() throws Exception {
      // Start a tx on A: put(k, v1), owners(k) = [B (primary) and C (backup)]
      // Block the commit on C so that it times out
      // Wait for the rollback command to be executed on B and C, and for the tx to end
      // Check that locks are released on B
      // Start another transaction on A: put(k, v2) with the same key
      // Check that the new transaction writes successfully
      // Allow the commit to proceed on C
      // Check that k=v2 everywhere
      StateSequencer sequencer = new StateSequencer();
      sequencer.logicalThread("tx1", "tx1:begin", "tx1:block_commit_on_backup", "tx1:after_rollback_on_primary",
            "tx1:after_rollback_on_backup", "tx1:resume_commit_on_backup", "tx1:after_commit_on_backup", "tx1:check");
      sequencer.logicalThread("tx2", "tx2:begin", "tx2:end");

      sequencer.order("tx1:after_rollback_on_backup", "tx2:begin", "tx2:end", "tx1:resume_commit_on_backup");

      advanceOnInterceptor(sequencer, cache(2), StateTransferInterceptor.class,
            matchCommand(VersionedCommitCommand.class).matchCount(0).build())
            .before("tx1:block_commit_on_backup", "tx1:resume_commit_on_backup").after("tx1:after_commit_on_backup");

      advanceOnInterceptor(sequencer, cache(1), StateTransferInterceptor.class,
            matchCommand(RollbackCommand.class).build())
            .after("tx1:after_rollback_on_primary");

      advanceOnInterceptor(sequencer, cache(2), StateTransferInterceptor.class,
            matchCommand(RollbackCommand.class).build())
            .after("tx1:after_rollback_on_backup");

      assertEquals(Arrays.asList(address(1), address(2)), cacheTopology(0).getDistribution(TEST_KEY).writeOwners());
      sequencer.advance("tx1:begin");

      tm(0).begin();
      cache(0).put(TEST_KEY, TX1_VALUE);
      try {
         tm(0).commit();
      } catch (RollbackException e) {
         log.debugf("Commit timed out as expected", e);
      }

      sequencer.advance("tx2:begin");
      LockManager lockManager1 = TestingUtil.extractLockManager(cache(1));
      assertFalse(lockManager1.isLocked(TEST_KEY));

      tm(0).begin();
      cache(0).put(TEST_KEY, TX2_VALUE);
      tm(0).commit();

      checkValue();
      sequencer.advance("tx2:end");

      sequencer.advance("tx1:check");
      checkValue();
   }

   private void checkValue() {
      for (Cache cache : caches()) {
         assertEquals(TX2_VALUE, cache.get(TEST_KEY));
      }
   }

   @Test(enabled = false, description = "Fix for this scenario is not implemented yet - rollback is asynchronous")
   public void testCommitDoesntWriteAfterTxEnd() throws Exception {
      // Start a tx on A: put(k, v1), owners(k) = [B (primary) and C (backup)]
      // Block the commit on C so that it times out
      // Wait for the rollback command to be executed on B and block before it executes on C
      // Check that k is still locked on B
      // Allow the commit to proceed on C
      // Allow the rollback to proceed on C
      // Check that k=v1 everywhere
      // Check that locks are released on B
      final StateSequencer sequencer = new StateSequencer();
      sequencer.logicalThread("tx1", "tx1:begin", "tx1:block_commit_on_backup", "tx1:after_rollback_on_primary",
            "tx1:block_rollback_on_backup", "tx1:resume_commit_on_backup", "tx1:after_commit_on_backup",
            "tx1:resume_rollback_on_backup", "tx1:after_rollback_on_backup", "tx1:check");

      advanceOnInterceptor(sequencer, cache(2), StateTransferInterceptor.class,
            matchCommand(CommitCommand.class).matchCount(0).build())
            .before("tx1:block_commit_on_backup", "tx1:resume_commit_on_backup").after("tx1:after_commit_on_backup");

      advanceOnInterceptor(sequencer, cache(1), StateTransferInterceptor.class,
            matchCommand(RollbackCommand.class).build())
            .after("tx1:after_rollback_on_primary");

      advanceOnInterceptor(sequencer, cache(2), StateTransferInterceptor.class,
            matchCommand(RollbackCommand.class).build())
            .before("tx1:block_rollback_on_backup").after("tx1:after_rollback_on_backup");

      assertEquals(Arrays.asList(address(1), address(2)), cacheTopology(0).getDistribution(TEST_KEY).writeOwners());
      Future<Object> lockCheckFuture = fork(() -> {
         sequencer.enter("tx1:resume_rollback_on_backup");
         try {
            assertTrue(TestingUtil.extractLockManager(cache(1)).isLocked(TEST_KEY));
         } finally {
            sequencer.exit("tx1:resume_rollback_on_backup");
         }
         return null;
      });


      sequencer.advance("tx1:begin");

      tm(0).begin();
      cache(0).put(TEST_KEY, TX1_VALUE);
      tm(0).commit();

      sequencer.advance("tx1:check");
      assertFalse(TestingUtil.extractLockManager(cache(1)).isLocked(TEST_KEY));
      lockCheckFuture.get(10, TimeUnit.SECONDS);
   }
}
