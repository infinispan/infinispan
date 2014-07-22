package org.infinispan.tx;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.infinispan.tx.recovery.RecoveryTestUtil.beginAndSuspendTx;
import static org.infinispan.tx.recovery.RecoveryTestUtil.commitTransaction;
import static org.infinispan.tx.recovery.RecoveryTestUtil.prepareTransaction;
import static org.testng.Assert.assertEquals;

/**
 * @author Mircea Markus
 */
@Test(groups = "functional", testName = "tx.ParticipantFailsAfterPrepareTest")
public class ParticipantFailsAfterPrepareTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder configuration = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      configuration
         .locking()
            .useLockStriping(false)
         .transaction()
            .transactionManagerLookup(new DummyTransactionManagerLookup())
            .useSynchronization(false)
            .recovery()
               .disable()
         .clustering()
            .stateTransfer()
               .fetchInMemoryState(false)
            .hash()
               .numOwners(3);
      createCluster(configuration, 4);
      waitForClusterToForm();
   }

   public void testNonOriginatorFailsAfterPrepare() throws Exception {
      final Object key = getKeyForCache(0);
      DummyTransaction dummyTransaction = beginAndSuspendTx(cache(0), key);
      prepareTransaction(dummyTransaction);

      int indexToKill = -1;
      //this tx spreads over 3 out of 4 nodes, let's find one that has the tx and kill it
      final List<Address> locate = advancedCache(0).getDistributionManager().getConsistentHash().locateOwners(key);
      for (int i = 3; i > 0; i--) {
         if (locate.contains(address(i))) {
            indexToKill = i;
            break;
         }
      }

      log.debug("indexToKill = " + indexToKill);
      assert indexToKill > 0;

      Address toKill = address(indexToKill);
      TestingUtil.killCacheManagers(manager(indexToKill));

      List<Cache> participants;
      participants = getAliveParticipants(indexToKill);

      TestingUtil.blockUntilViewsReceived(60000, false, participants);
      TestingUtil.waitForRehashToComplete(participants);

      //one of the participants must not have a prepare on it
      boolean noLocks = false;
      for (Cache c : participants) {
         if (TestingUtil.extractLockManager(c).getNumberOfLocksHeld() == 0) noLocks = true;
      }
      assert noLocks;

      log.trace("About to commit. Killed node is: " + toKill);

      try {
         commitTransaction(dummyTransaction);
      } finally {
         //now check weather all caches have the same content and no locks acquired
         for (Cache c : participants) {
            assertEquals(TestingUtil.extractLockManager(c).getNumberOfLocksHeld(), 0);
            assertEquals(c.keySet().size(), 1);
         }
      }
   }

   private List<Cache> getAliveParticipants(int indexToKill) {
      List<Cache> participants = new ArrayList<Cache>();
      for (int i = 0; i < 4; i++) {
         if (i == indexToKill) continue;
         participants.add(cache(i));
      }
      return participants;
   }
}