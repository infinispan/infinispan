package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnInterceptor;
import static org.infinispan.test.concurrent.StateSequencerUtil.matchCommand;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

/**
 * Test that a commit command that has timed out on a backup owner cannot write entries after the locks have been
 * released on the primary owner.
 */
@Test(groups = "functional", testName = "statetransfer.PrepareTimeoutTest")
@CleanupAfterMethod
public class PrepareTimeoutTest extends MultipleCacheManagersTest {
   private static final String TEST_KEY = "key";
   private static final String TX1_VALUE = "value1";
   private static final java.lang.Object TX2_VALUE = "value2";
   public static final int COMPLETED_TX_TIMEOUT = 2000;

   @Override
   protected void createCacheManagers() throws Throwable {
      ControlledConsistentHashFactory consistentHashFactory = new ControlledConsistentHashFactory(1, 2);

      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.clustering().cacheMode(CacheMode.DIST_SYNC);
      builder.clustering().sync().replTimeout(2000);
      builder.clustering().hash().numSegments(1).consistentHashFactory(consistentHashFactory);
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      builder.transaction().completedTxTimeout(COMPLETED_TX_TIMEOUT);

      addClusterEnabledCacheManager(builder);
      addClusterEnabledCacheManager(builder);
      addClusterEnabledCacheManager(builder);
      waitForClusterToForm();
   }

   public void testCommitDoesntWriteAfterRollback() throws Exception {
      // Start a tx on A: put(k, v1), owners(k) = [B (primary) and C (backup)]
      // Block the prepare on B and C so that it times out
      // Wait for the rollback command to be executed on B and C
      // Unblock the prepare on B and C
      // Check that there are no locked keys or remote transactions on B and C
      StateSequencer sequencer = new StateSequencer();
      sequencer.logicalThread("main", "main:start", "main:check");
      sequencer.logicalThread("primary", "primary:block_prepare", "primary:after_rollback", "primary:resume_prepare", 
            "primary:after_prepare");
      sequencer.logicalThread("backup", "backup:block_prepare", "backup:after_rollback", "backup:resume_prepare",
            "backup:after_prepare");

      sequencer.order("main:start", "primary:block_prepare", "primary:after_prepare", "main:check");
      sequencer.order("main:start", "backup:block_prepare", "backup:after_prepare", "main:check");

      advanceOnInterceptor(sequencer, cache(1), StateTransferInterceptor.class,
            matchCommand(PrepareCommand.class).matchCount(0).build())
            .before("primary:block_prepare", "primary:resume_prepare").after("primary:after_prepare");

      advanceOnInterceptor(sequencer, cache(1), StateTransferInterceptor.class,
            matchCommand(RollbackCommand.class).build())
            .after("primary:after_rollback");

      advanceOnInterceptor(sequencer, cache(2), StateTransferInterceptor.class,
            matchCommand(PrepareCommand.class).matchCount(0).build())
            .before("backup:block_prepare", "backup:resume_prepare").after("backup:after_prepare");

      advanceOnInterceptor(sequencer, cache(2), StateTransferInterceptor.class,
            matchCommand(RollbackCommand.class).build())
            .after("backup:after_rollback");


      assertEquals(Arrays.asList(address(1), address(2)), advancedCache(0).getDistributionManager().locate(TEST_KEY));
      sequencer.advance("main:start");

      tm(0).begin();
      cache(0).put(TEST_KEY, TX1_VALUE);
      try {
         tm(0).commit();
         fail("Exception expected during commit");
      } catch (Exception e) {
         // expected
      }

      tm(0).begin();
      cache(0).put(TEST_KEY, TX2_VALUE);
      GlobalTransaction gtx1 = transactionTable(0).getLocalTransaction(tm(0).getTransaction()).getGlobalTransaction();
      tm(0).commit();

      // Wait for the 1st tx to be removed from the completed txs table
      Thread.sleep(COMPLETED_TX_TIMEOUT + 1000);

      assertTrue(transactionTable(1).isTransactionCompleted(gtx1));
      assertTrue(transactionTable(2).isTransactionCompleted(gtx1));

      sequencer.advance("main:check");

      LockManager lockManager1 = TestingUtil.extractLockManager(cache(1));
      assertFalse(lockManager1.isLocked(TEST_KEY));

      assertFalse(transactionTable(1).containRemoteTx(gtx1));
      assertFalse(transactionTable(2).containRemoteTx(gtx1));

      for (Cache cache : caches()) {
         assertEquals(TX2_VALUE, cache.get(TEST_KEY));
      }
   }
}
