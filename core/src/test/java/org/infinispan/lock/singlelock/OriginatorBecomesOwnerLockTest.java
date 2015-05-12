package org.infinispan.lock.singlelock;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;


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
   private boolean waitForStateTransfer = true;
   // TODO Tests fails with SuspectException if stopCacheOnly == false because of https://issues.jboss.org/browse/ISPN-2402
   private boolean stopCacheOnly = true;

   @Override
   protected void createCacheManagers() throws Throwable {
      configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true, true);
      configurationBuilder.transaction().transactionManagerLookup(new DummyTransactionManagerLookup());
      configurationBuilder.clustering().sync().replTimeout(30000, TimeUnit.MILLISECONDS);
      configurationBuilder.clustering().hash().l1().disable().locking().lockAcquisitionTimeout(1000);
      configurationBuilder.clustering().stateTransfer().fetchInMemoryState(true);
      createCluster(configurationBuilder, 3);
      waitForClusterToForm();

      originatorCache = cache(ORIGINATOR_INDEX);
      killedCache = cache(KILLED_INDEX);
      otherCache = cache(OTHER_INDEX);
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
      ControlledRpcManager controlledRpcManager = installControlledRpcManager(PrepareCommand.class);
      final DummyTransactionManager tm = dummyTm(ORIGINATOR_INDEX);

      Future<DummyTransaction> f = fork(new Callable<DummyTransaction>() {
         @Override
         public DummyTransaction call() throws Exception {
            tm.begin();
            originatorCache.put(key, "value");
            DummyTransaction tx = tm.getTransaction();

            boolean success = tx.runPrepare();
            assertTrue(success);
            tm.suspend();
            return tx;
         }
      });

      // Allow the tx thread to send the prepare command to the owners
      Thread.sleep(2000);

      log.trace("Lock transfer happens here");
      killCache();

      log.trace("Allow the prepare RPC to proceed");
      controlledRpcManager.stopBlocking();
      // Ensure the prepare finished on the other node
      DummyTransaction tx = f.get();
      log.tracef("Prepare finished");

      checkNewTransactionFails(key);

      log.trace("About to commit existing transactions.");
      tm.resume(tx);
      tx.runCommit(false);

      // read the data from the container, just to make sure all replicas are correctly set
      checkValue(key, "value");
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
      final DummyTransactionManager tm = dummyTm(ORIGINATOR_INDEX);

      tm.begin();
      originatorCache.put(key, "value");
      DummyTransaction tx = tm.getTransaction();

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
      ControlledRpcManager controlledRpcManager = installControlledRpcManager(CommitCommand.class);
      final DummyTransactionManager tm = dummyTm(ORIGINATOR_INDEX);

      Future<DummyTransaction> f = fork(new Callable<DummyTransaction>() {
         @Override
         public DummyTransaction call() throws Exception {
            tm.begin();
            originatorCache.put(key, "value");
            final DummyTransaction tx = tm.getTransaction();
            final boolean success = tx.runPrepare();
            assert success;

            log.trace("About to commit transaction.");
            tx.runCommit(false);
            return null;
         }
      });

      // Allow the tx thread to send the commit to the owners
      Thread.sleep(2000);

      log.trace("Lock transfer happens here");
      killCache();

      log.trace("Allow the commit RPC to proceed");
      controlledRpcManager.stopBlocking();
      // Ensure the commit finished on the other node
      f.get();
      log.tracef("Commit finished");

      // read the data from the container, just to make sure all replicas are correctly set
      checkValue(key, "value");

      assertNoLocksOrTxs(key, originatorCache);
      assertNoLocksOrTxs(key, otherCache);
   }


   private void assertNoLocksOrTxs(Object key, Cache<Object, String> cache) {
      assertEventuallyNotLocked(originatorCache, key);

      final TransactionTable transactionTable = TestingUtil.extractComponent(cache, TransactionTable.class);

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return transactionTable.getLocalTxCount() == 0 && transactionTable.getRemoteTxCount() == 0;
         }
      });
   }

   private ControlledRpcManager installControlledRpcManager(Class<? extends VisitableCommand> commandClass) {
      ControlledRpcManager controlledRpcManager = new ControlledRpcManager(
            originatorCache.getAdvancedCache().getRpcManager());
      controlledRpcManager.blockBefore(commandClass);
      TestingUtil.replaceComponent(originatorCache, RpcManager.class, controlledRpcManager, true);
      return controlledRpcManager;
   }

   private void killCache() {
      if (stopCacheOnly) {
         killedCache.stop();
      } else {
         manager(KILLED_INDEX).stop();
      }
      if (waitForStateTransfer) {
         TestingUtil.waitForRehashToComplete(originatorCache, otherCache);
      }
   }

   private void checkValue(Object key, String value) {
      if (!waitForStateTransfer) {
         TestingUtil.waitForRehashToComplete(originatorCache, otherCache);
      }
      log.tracef("Checking key: %s", key);
      InternalCacheEntry d0 = advancedCache(ORIGINATOR_INDEX).getDataContainer().get(key);
      InternalCacheEntry d1 = advancedCache(OTHER_INDEX).getDataContainer().get(key);
      assertEquals(d0.getValue(), value);
      assertEquals(d1.getValue(), value);
   }

   private void checkNewTransactionFails(Object key) throws NotSupportedException, SystemException, HeuristicMixedException, HeuristicRollbackException {
      DummyTransactionManager otherTM = dummyTm(OTHER_INDEX);
      otherTM.begin();
      otherCache.put(key, "should fail");
      try {
         otherTM.commit();
         fail("RollbackException should have been thrown here.");
      } catch (RollbackException e) {
         //expected
      }
   }


   private DummyTransactionManager dummyTm(int cacheIndex) {
      return (DummyTransactionManager) tm(cacheIndex);
   }
}
