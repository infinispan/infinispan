package org.infinispan.tx;

import static org.infinispan.test.TestingUtil.extractLockManager;
import static org.infinispan.tx.recovery.RecoveryTestUtil.beginAndSuspendTx;
import static org.infinispan.tx.recovery.RecoveryTestUtil.commitTransaction;
import static org.infinispan.tx.recovery.RecoveryTestUtil.prepareTransaction;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 */
@Test(groups = "functional", testName = "tx.ParticipantFailsAfterPrepareTest")
@CleanupAfterMethod
public class ParticipantFailsAfterPrepareTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configuration = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      configuration
            .locking()
            .useLockStriping(false)
            .transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .lockingMode(LockingMode.OPTIMISTIC)
            .useSynchronization(false)
            .recovery()
            .disable()
            .clustering()
            .stateTransfer()
            .fetchInMemoryState(false)
            .hash()
            .numOwners(3);
      createCluster(TestDataSCI.INSTANCE, configuration, 4);
      waitForClusterToForm();
   }

   public void testNonOriginatorPrimaryFailsAfterPrepare() throws Exception {
      testNonOriginatorFailsAfterPrepare(1, 1);
   }

   public void testNonOriginatorBackupFailsAfterPrepare() throws Exception {
      testNonOriginatorFailsAfterPrepare(0, 2);
   }

   private void testNonOriginatorFailsAfterPrepare(int primaryOwnerIndex, int toKillIndex) throws Exception {
      Address originator = address(0);
      Address primaryOwner = address(primaryOwnerIndex);
      Address toKill = address(toKillIndex);
      MagicKey key = new MagicKey(cache(primaryOwnerIndex), cache(toKillIndex));

      Cache<Object, Object> originatorCache = cache(0);
      DistributionManager dm0 = advancedCache(0).getDistributionManager();
      EmbeddedTransaction dummyTransaction = beginAndSuspendTx(originatorCache, key);
      prepareTransaction(dummyTransaction);

      log.debugf("Killing %s, key owners are %s", toKill, dm0.getCacheTopology().getWriteOwners(key));
      killMember(toKillIndex);
      log.debugf("Killed %s, key owners are %s", toKill, dm0.getCacheTopology().getWriteOwners(key));

      for (Cache<Object, Object> c : caches()) {
         if (!address(c).equals(originator)) {
            TransactionTable nonOwnerTxTable = TestingUtil.extractComponent(c, TransactionTable.class);
            assertEquals(1, nonOwnerTxTable.getRemoteGlobalTransaction().size());
         }

         // If the primary owner changes, the lock is not re-acquired on the new primary owner
         // However, the old primary owner doesn't release the lock - it's just ignored by new transactions
         boolean expectLocked = address(c).equals(primaryOwner);
         boolean locked = extractLockManager(c).isLocked(key);
         log.tracef("On %s, locked = %s", address(c), locked);
         assertEquals(expectLocked, locked);
      }

      log.trace("About to commit. Killed node is: " + toKill);

      commitTransaction(dummyTransaction);

      //now check whether all caches have the same content and no locks acquired
      for (Cache<?, ?> c : caches()) {
         // If the new owner becomes the primary, it will release the key asynchronously
         assertEventuallyNotLocked(c, key);
         assertEquals(1, c.keySet().size());
      }
   }
}
