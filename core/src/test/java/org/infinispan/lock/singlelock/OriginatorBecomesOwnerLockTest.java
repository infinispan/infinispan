package org.infinispan.lock.singlelock;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.NotSupportedException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;

import org.infinispan.Cache;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.commands.statetransfer.StateTransferGetTransactionsCommand;
import org.infinispan.commands.statetransfer.StateTransferStartCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.ControlledRpcManager;
import org.infinispan.util.ReplicatedControlledConsistentHashFactory;
import org.testng.annotations.Test;


/**
 * Test what happens if the originator becomes an owner during a prepare or commit RPC.
 * @since 5.2
 */
@Test(groups = "functional", testName = "lock.singlelock.OriginatorBecomesOwnerLockTest")
@CleanupAfterMethod
public class OriginatorBecomesOwnerLockTest extends MultipleCacheManagersTest {

   private ConfigurationBuilder configurationBuilder;
   private static final int ORIGINATOR_INDEX = 0;
   private static final int OTHER_INDEX = 1;
   private static final int KILLED_INDEX = 2;
   private Cache<Object, String> originatorCache;
   private Cache<Object, String> killedCache;
   private Cache<Object, String> otherCache;

   // Pseudo-configuration
   // TODO Test fails (expected RollbackException isn't raised) if waitForStateTransfer == false because of https://issues.jboss.org/browse/ISPN-2510
   private final boolean waitForStateTransfer = true;
   // TODO Tests fails with SuspectException if stopCacheOnly == false because of https://issues.jboss.org/browse/ISPN-2402
   private final boolean stopCacheOnly = true;

   @Override
   protected void createCacheManagers() throws Throwable {
      configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true, true);
      configurationBuilder.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      configurationBuilder.clustering().remoteTimeout(30000, TimeUnit.MILLISECONDS);
      configurationBuilder.clustering().hash().l1().disable();
      configurationBuilder.locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());
      configurationBuilder.clustering().stateTransfer().fetchInMemoryState(true);
      ControlledConsistentHashFactory consistentHashFactory =
            new ControlledConsistentHashFactory.Default(new int[][]{{KILLED_INDEX, ORIGINATOR_INDEX},
                  {KILLED_INDEX, OTHER_INDEX}});
      configurationBuilder.clustering().hash().numSegments(2).consistentHashFactory(consistentHashFactory);

      GlobalConfigurationBuilder globalBuilder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      globalBuilder.serialization().addContextInitializers(TestDataSCI.INSTANCE, ControlledConsistentHashFactory.SCI.INSTANCE,
            ReplicatedControlledConsistentHashFactory.SCI.INSTANCE);
      createCluster(globalBuilder, configurationBuilder, 3);
      waitForClusterToForm();

      originatorCache = cache(ORIGINATOR_INDEX);
      killedCache = cache(KILLED_INDEX);
      otherCache = cache(OTHER_INDEX);
      // Set up the consistent hash after node 1 is killed
      consistentHashFactory.setOwnerIndexes(new int[][]{{ORIGINATOR_INDEX, OTHER_INDEX},
            {OTHER_INDEX, ORIGINATOR_INDEX}});
      // TODO Add another test method with ownership changing from [KILLED_INDEX, OTHER_INDEX] to [ORIGINATOR_INDEX, OTHER_INDEX]
      // i.e. the originator is a non-owner at first, and becomes the primary owner when the prepare is retried
   }


   public void testOriginatorBecomesPrimaryOwnerDuringPrepare() throws Exception {
      Object key = new MagicKey("primary", cache(KILLED_INDEX), cache(ORIGINATOR_INDEX));
      testLockMigrationDuringPrepare(key);
   }

   public void testOriginatorBecomesBackupOwnerDuringPrepare() throws Exception {
      Object key = new MagicKey("backup", cache(KILLED_INDEX), cache(OTHER_INDEX));
      testLockMigrationDuringPrepare(key);
   }

   private void testLockMigrationDuringPrepare(final Object key) throws Exception {
      ControlledRpcManager controlledRpcManager = ControlledRpcManager.replaceRpcManager(originatorCache);
      controlledRpcManager.excludeCommands(StateTransferStartCommand.class, StateTransferGetTransactionsCommand.class, StateResponseCommand.class);
      final EmbeddedTransactionManager tm = embeddedTm(ORIGINATOR_INDEX);

      Future<EmbeddedTransaction> f = fork(() -> {
         tm.begin();
         originatorCache.put(key, "value");
         EmbeddedTransaction tx = tm.getTransaction();

         boolean success = tx.runPrepare();
         assertTrue(success);
         tm.suspend();
         return tx;
      });

      if (!originatorCache.getAdvancedCache().getDistributionManager().getCacheTopology().isReadOwner(key)) {
         controlledRpcManager.expectCommand(ClusteredGetCommand.class).send().receiveAll();
      }

      ControlledRpcManager.BlockedRequest blockedPrepare = controlledRpcManager.expectCommand(PrepareCommand.class);

      // Allow the tx thread to send the prepare command to the owners
      Thread.sleep(2000);

      log.trace("Lock transfer happens here");
      killCache();

      log.trace("Allow the prepare RPC to proceed");
      blockedPrepare.send().receiveAll();
      // Also allow the retry to proceed
      controlledRpcManager.expectCommand(PrepareCommand.class).send().receiveAll();

      // Ensure the prepare finished on the other node
      EmbeddedTransaction tx = f.get();
      log.tracef("Prepare finished");

      checkNewTransactionFails(key);

      log.trace("About to commit existing transactions.");
      controlledRpcManager.excludeCommands(CommitCommand.class, TxCompletionNotificationCommand.class);
      tm.resume(tx);
      tx.runCommit(false);

      // read the data from the container, just to make sure all replicas are correctly set
      checkValue(key, "value");

      controlledRpcManager.stopBlocking();
   }


   public void testOriginatorBecomesPrimaryOwnerAfterPrepare() throws Exception {
      Object key = new MagicKey("primary", cache(KILLED_INDEX), cache(ORIGINATOR_INDEX));
      testLockMigrationAfterPrepare(key);
   }

   public void testOriginatorBecomesBackupOwnerAfterPrepare() throws Exception {
      Object key = new MagicKey("backup", cache(KILLED_INDEX), cache(OTHER_INDEX));
      testLockMigrationAfterPrepare(key);
   }

   private void testLockMigrationAfterPrepare(Object key) throws Exception {
      final EmbeddedTransactionManager tm = embeddedTm(ORIGINATOR_INDEX);

      tm.begin();
      originatorCache.put(key, "value");
      EmbeddedTransaction tx = tm.getTransaction();

      boolean prepareSuccess = tx.runPrepare();
      assert prepareSuccess;

      tm.suspend();

      log.trace("Lock transfer happens here");
      killCache();

      checkNewTransactionFails(key);

      log.trace("About to commit existing transaction.");
      tm.resume(tx);
      tx.runCommit(false);

      // read the data from the container, just to make sure all replicas are correctly set
      checkValue(key, "value");
   }


   public void testOriginatorBecomesPrimaryOwnerDuringCommit() throws Exception {
      Object key = new MagicKey("primary", cache(KILLED_INDEX), cache(ORIGINATOR_INDEX));
      testLockMigrationDuringCommit(key);
   }

   public void testOriginatorBecomesBackupOwnerDuringCommit() throws Exception {
      Object key = new MagicKey("backup", cache(KILLED_INDEX), cache(OTHER_INDEX));
      testLockMigrationDuringCommit(key);
   }

   private void testLockMigrationDuringCommit(final Object key) throws Exception {
      ControlledRpcManager controlledRpcManager = ControlledRpcManager.replaceRpcManager(originatorCache);
      controlledRpcManager.excludeCommands(StateTransferStartCommand.class, StateTransferGetTransactionsCommand.class, StateResponseCommand.class);
      final EmbeddedTransactionManager tm = embeddedTm(ORIGINATOR_INDEX);

      Future<EmbeddedTransaction> f = fork(() -> {
         tm.begin();
         originatorCache.put(key, "value");
         final EmbeddedTransaction tx = tm.getTransaction();
         final boolean success = tx.runPrepare();
         assert success;

         log.trace("About to commit transaction.");
         tx.runCommit(false);
         return null;
      });

      if (!originatorCache.getAdvancedCache().getDistributionManager().getCacheTopology().isReadOwner(key)) {
         controlledRpcManager.expectCommand(ClusteredGetCommand.class).send().receiveAll();
      }
      controlledRpcManager.expectCommand(PrepareCommand.class).send().receiveAll();

      // Wait for the tx thread to block sending the commit
      ControlledRpcManager.BlockedRequest blockedCommit = controlledRpcManager.expectCommand(CommitCommand.class);

      log.trace("Lock transfer happens here");
      killCache();

      log.trace("Allow the commit RPC to proceed");
      blockedCommit.send().receiveAll();

      // Process the retry and the completion notification normally
      controlledRpcManager.expectCommand(CommitCommand.class).send().receiveAll();
      controlledRpcManager.expectCommand(TxCompletionNotificationCommand.class).send();

      // Ensure the commit finished on the other node
      f.get(30, TimeUnit.SECONDS);
      log.tracef("Commit finished");

      // read the data from the container, just to make sure all replicas are correctly set
      checkValue(key, "value");

      assertNoLocksOrTxs(key, originatorCache);
      assertNoLocksOrTxs(key, otherCache);

      controlledRpcManager.stopBlocking();
   }


   private void assertNoLocksOrTxs(Object key, Cache<Object, String> cache) {
      assertEventuallyNotLocked(originatorCache, key);

      final TransactionTable transactionTable = TestingUtil.extractComponent(cache, TransactionTable.class);

      eventuallyEquals(0, transactionTable::getLocalTxCount);
      eventuallyEquals(0, transactionTable::getRemoteTxCount);
   }

   private void killCache() {
      if (stopCacheOnly) {
         killedCache.stop();
      } else {
         manager(KILLED_INDEX).stop();
      }
      if (waitForStateTransfer) {
         TestingUtil.waitForNoRebalance(originatorCache, otherCache);
      }
   }

   private void checkValue(Object key, String value) {
      if (!waitForStateTransfer) {
         TestingUtil.waitForNoRebalance(originatorCache, otherCache);
      }
      log.tracef("Checking key: %s", key);
      InternalCacheEntry d0 = advancedCache(ORIGINATOR_INDEX).getDataContainer().get(key);
      InternalCacheEntry d1 = advancedCache(OTHER_INDEX).getDataContainer().get(key);
      assertEquals(d0.getValue(), value);
      assertEquals(d1.getValue(), value);
   }

   private void checkNewTransactionFails(Object key) throws NotSupportedException, SystemException, HeuristicMixedException, HeuristicRollbackException {
      EmbeddedTransactionManager otherTM = embeddedTm(OTHER_INDEX);
      otherTM.begin();
      otherCache.put(key, "should fail");
      try {
         otherTM.commit();
         fail("RollbackException should have been thrown here.");
      } catch (RollbackException e) {
         //expected
      }
   }


   private EmbeddedTransactionManager embeddedTm(int cacheIndex) {
      return (EmbeddedTransactionManager) tm(cacheIndex);
   }
}
