package org.infinispan.distribution.rehash;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnInboundRpc;
import static org.infinispan.test.concurrent.StateSequencerUtil.matchCommand;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.Future;

import javax.transaction.xa.XAException;

import org.infinispan.commands.tx.VersionedPrepareCommand;
import org.infinispan.commons.TimeoutException;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.Test;

import jakarta.transaction.RollbackException;
import jakarta.transaction.Status;

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
      EmbeddedTransaction tx1 = (EmbeddedTransaction) tm(0).suspend();
      tx1.runPrepare();

      advanceOnInboundRpc(ss, cache(1), matchCommand(VersionedPrepareCommand.class).build())
            .before("block_prepare", "resume_prepare");

      Future<EmbeddedTransaction> tx2Future = fork(() -> {
         tm(0).begin();
         cache(0).put("k", "v2");
         EmbeddedTransaction tx2 = (EmbeddedTransaction) tm(0).suspend();
         tx2.runPrepare();
         return tx2;
      });

      ss.enter("crash_primary");
      killMember(1);
      ss.exit("crash_primary");

      // tx2 prepare times out trying to acquire the lock, but does not throw an exception at this time
      EmbeddedTransaction tx2 = tx2Future.get(30, SECONDS);
      assertEquals(Status.STATUS_MARKED_ROLLBACK, tx2.getStatus());
      Exceptions.expectException(RollbackException.class, XAException.class, TimeoutException.class, () -> tx2.runCommit(false));

      // tx1 should commit successfully
      tx1.runCommit(false);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder config = new ConfigurationBuilder();
      config.clustering().cacheMode(CacheMode.DIST_SYNC);
      config.transaction().lockingMode(LockingMode.OPTIMISTIC);
      config.clustering().locking().lockAcquisitionTimeout(2, SECONDS);
      config.clustering().hash().numSegments(1)
            .consistentHashFactory(new ControlledConsistentHashFactory.Default(1, 0));
      config.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .cacheStopTimeout(1, SECONDS);
      createCluster(ControlledConsistentHashFactory.SCI.INSTANCE, config, 2);
      waitForClusterToForm();
   }
}
