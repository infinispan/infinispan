package org.infinispan.distribution.rehash;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnInboundRpc;
import static org.infinispan.test.concurrent.StateSequencerUtil.matchCommand;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.Future;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.Test;

/**
 * Test that if the primary owner crashes while two transactions are in the prepare phase, only one of them will be able
 * to commit the transaction.
 *
 * @author Dan Berindei
 * @since 8.0
 */
@Test(groups = "functional", testName = "distribution.rehash.OptimisticPrimaryOwnerCrashDuringPrepareTest")
@CleanupAfterMethod
public class OptimisticPrimaryOwnerCrashDuringPrepareTest extends MultipleCacheManagersTest {

   public void testPrimaryOwnerCrash() throws Exception {
      // cache 0 is the originator and backup, cache 1 is the primary owner
      StateSequencer ss = new StateSequencer();
      ss.logicalThread("main", "block_prepare", "crash_primary", "resume_prepare");

      tm(0).begin();
      cache(0).put("k", "v1");
      DummyTransaction tx1 = (DummyTransaction) tm(0).suspend();
      tx1.runPrepare();

      advanceOnInboundRpc(ss, cache(1), matchCommand(PrepareCommand.class).build())
            .before("block_prepare", "resume_prepare");

      Future<DummyTransaction> tx2Future = fork(() -> {
         tm(0).begin();
         cache(0).put("k", "v2");
         DummyTransaction tx2 = (DummyTransaction) tm(0).suspend();
         tx2.runPrepare();
         return tx2;
      });

      ss.enter("crash_primary");
      killMember(1);
      ss.exit("crash_primary");

      DummyTransaction tx2 = tx2Future.get(10, SECONDS);
      try {
         tx2.runCommit(false);
         fail("tx2 should not be able to commit");
      } catch (Exception e) {
         log.tracef(e, "Received expected exception");
      }

      tx1.runCommit(false);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.clustering().cacheMode(CacheMode.DIST_SYNC);
      config.transaction().lockingMode(LockingMode.OPTIMISTIC);
      config.clustering().hash().numSegments(1)
            .consistentHashFactory(new ControlledConsistentHashFactory(1, 0));
      config.transaction().transactionManagerLookup(new DummyTransactionManagerLookup())
            .cacheStopTimeout(1, SECONDS);
      createCluster(config, 2);
      waitForClusterToForm();
   }
}
