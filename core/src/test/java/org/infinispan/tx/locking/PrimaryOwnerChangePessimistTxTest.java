package org.infinispan.tx.locking;

import static org.infinispan.commons.test.Exceptions.expectException;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.TimeoutException;
import org.infinispan.commons.test.ExceptionRunnable;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.MagicKey;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.Test;

import jakarta.transaction.NotSupportedException;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;

/**
 * Reproducer for ISPN-7140
 * <p>
 * Issue: with pessimistic cache, if the primary owner change, the new primary owner can start a transaction and acquire
 * the lock of key (previously locked) without checking for existing transactions
 *
 * @author Pedro Ruivo
 * @since 13.0
 */
@CleanupAfterMethod
@Test(groups = "functional", testName = "tx.locking.PrimaryOwnerChangePessimistTxTest")
public class PrimaryOwnerChangePessimistTxTest extends MultipleCacheManagersTest {
   private ControlledConsistentHashFactory.Default factory;

   @Override
   protected void createCacheManagers() throws Throwable {
      factory = new ControlledConsistentHashFactory.Default(new int[][]{{0, 1}, {0, 2}});
      createClusteredCaches(3, ControlledConsistentHashFactory.SCI.INSTANCE, configuration(), new TransportFlags().withFD(true));
   }

   private ConfigurationBuilder configuration() {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cb.transaction().lockingMode(LockingMode.PESSIMISTIC);
      cb.clustering().hash().numSegments(2).consistentHashFactory(factory);
      return cb;
   }

   public void testNodeLeaving() throws Exception {
      testPrimaryChange(this::nodeLeaves);
   }

   public void testNodeJoining() throws Exception {
      testPrimaryChange(this::nodeJoins);
   }

   /*
    ISPN-7140 test case
    1. TX1 originating on A acquires lock for key X, A is primary owner and either B or C is backup owner
    2. C is killed and B becomes primary owner of key X
      (the order of owners for a segment can change even if the set of owners stays the same)
    3. TX2 originating on B acquires lock for key X, B is now primary owner
    4. TX1 commits the tx, Prepare is sent with the new topology id so it commits fine
    5. TX2 also commits the transaction
    */
   private void testPrimaryChange(ExceptionRunnable topologyChange) throws Exception {
      MagicKey backupKey = new MagicKey(cache(0), cache(1));
      MagicKey nonOwnerKey = new MagicKey(cache(0), cache(2));

      // node0 is the primary owner
      assertPrimaryOwner(backupKey, 0);
      tm(0).begin();
      cache(0).put(backupKey, "value-0");
      Transaction tx0 = tm(0).suspend();

      tm(0).begin();
      advancedCache(0).lock(nonOwnerKey);
      Transaction tx1 = tm(0).suspend();

      // expect keys to be locked on primary owner
      assertLocked(0, backupKey);
      assertLocked(0, nonOwnerKey);

      // switch primary owner: node1
      factory.setOwnerIndexes(new int[][]{{1, 0}, {1, 0}});

      topologyChange.run();

      assertPrimaryOwner(backupKey, 1);
      assertPrimaryOwner(nonOwnerKey, 1);

      AdvancedCache<Object, Object> zeroTimeoutCache1 = advancedCache(1).withFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT);
      assertPutTimeout(backupKey, zeroTimeoutCache1);
      assertLockTimeout(backupKey, zeroTimeoutCache1);
      assertPutTimeout(nonOwnerKey, zeroTimeoutCache1);
      assertLockTimeout(nonOwnerKey, zeroTimeoutCache1);

      tm(0).resume(tx0);
      tm(0).commit();

      tm(0).resume(tx1);
      tm(0).commit();

      assertEquals("value-0", cache(0).get(backupKey));
      assertEquals("value-0", cache(1).get(backupKey));
      assertNull(cache(0).get(nonOwnerKey));
      assertNull(cache(1).get(nonOwnerKey));
   }

   private void nodeLeaves() {
      killMember(2);
   }

   private void nodeJoins() {
      addClusterEnabledCacheManager(ControlledConsistentHashFactory.SCI.INSTANCE, configuration(), new TransportFlags().withFD(true));
      waitForClusterToForm();
   }

   private void assertPutTimeout(MagicKey lockedKey1, AdvancedCache<Object, Object> zeroTimeoutCache)
         throws NotSupportedException, SystemException {
      tm(1).begin();
      expectException(TimeoutException.class, () -> zeroTimeoutCache.put(lockedKey1, "value-1"));
      tm(1).rollback();
   }

   private void assertLockTimeout(MagicKey lockedKey1, AdvancedCache<Object, Object> zeroTimeoutCache)
         throws NotSupportedException, SystemException {
      tm(1).begin();
      expectException(TimeoutException.class, () -> zeroTimeoutCache.lock(lockedKey1));
      tm(1).rollback();
   }

   private void assertPrimaryOwner(MagicKey key, int index) {
      DistributionManager dm = cache(index).getAdvancedCache().getDistributionManager();
      assertTrue(dm.getCacheTopology().getDistribution(key).isPrimary());
   }
}
