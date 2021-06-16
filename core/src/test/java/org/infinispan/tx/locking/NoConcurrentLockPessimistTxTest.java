package org.infinispan.tx.locking;

import static org.infinispan.commons.test.Exceptions.expectException;
import static org.infinispan.test.TestingUtil.k;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;

import javax.transaction.Transaction;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.LockingMode;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.concurrent.TimeoutException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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
@Test(groups = "functional", testName = "tx.locking.NoConcurrentLockPessimistTxTest")
public class NoConcurrentLockPessimistTxTest extends MultipleCacheManagersTest {

   private final ControlledConsistentHashFactory.Default factory = new ControlledConsistentHashFactory.Default(0, 1);

   @Override
   protected void createCacheManagers() throws Throwable {
      factory.setOwnerIndexes(0, 2);
      createClusteredCaches(3, configuration(), new TransportFlags().withFD(true));
   }

   private ConfigurationBuilder configuration() {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      cb.transaction().lockingMode(LockingMode.PESSIMISTIC);
      cb.clustering().hash().numSegments(1).consistentHashFactory(factory);
      return cb;
   }

   @DataProvider(name = "put-lock-data")
   protected static Object[][] data() {
      return new Object[][]{{true}, {false}};
   }


   /*
    ISPN-7140 test case
    1. TX1 originating on A acquires lock for key X, A is primary owner
    2. C is killed and B becomes primary owner of key X
    3. TX2 originating on B acquires lock for key X, B is now primary owner
    4. TX1 commits the tx, Prepare is sent with the new topology id so it commits fine
    5. TX2 also commits the transaction
    */
   @Test(dataProvider = "put-lock-data")
   public void testNodeLeaving(Method method, boolean useLock) throws Exception {
      final String key = k(method);

      // node0 is the primary owner
      assertPrimaryOwner(key, 0);
      tm(0).begin();
      cache(0).put(key, "value-0");
      final Transaction tx0 = tm(0).suspend();

      // expect key to be locked on primary owner
      assertLocked(0, key);

      // switch primary owner: node1
      factory.setOwnerIndexes(1, 0);

      killMember(2);
      assertPrimaryOwner(key, 1);

      tm(1).begin();
      if (useLock) {
         expectException(TimeoutException.class, () -> cache(1).getAdvancedCache().withFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT).lock(key));
      } else {
         expectException(TimeoutException.class, () -> cache(1).getAdvancedCache().withFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT).put(key, "value-1"));
      }
      tm(1).rollback();

      tm(0).resume(tx0);
      tm(0).commit();

      assertEquals("value-0", cache(0).get(key));
      assertEquals("value-0", cache(1).get(key));
   }

   @Test(dataProvider = "put-lock-data")
   public void testNodeJoining(Method method, boolean useLock) throws Exception {
      final String key = k(method);

      // node0 is the primary owner
      assertPrimaryOwner(key, 0);
      tm(0).begin();
      cache(0).put(key, "value-0");
      final Transaction tx0 = tm(0).suspend();

      // expect key to be locked on primary owner
      assertLocked(0, key);

      // switch primary owner: node1
      factory.setOwnerIndexes(1, 0);

      addClusterEnabledCacheManager(configuration(), new TransportFlags().withFD(true));
      waitForClusterToForm();
      assertPrimaryOwner(key, 1);

      tm(1).begin();
      if (useLock) {
         expectException(TimeoutException.class, () -> cache(1).getAdvancedCache().withFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT).lock(key));
      } else {
         expectException(TimeoutException.class, () -> cache(1).getAdvancedCache().withFlags(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT).put(key, "value-1"));
      }
      tm(1).rollback();

      tm(0).resume(tx0);
      tm(0).commit();

      assertEquals("value-0", cache(0).get(key));
      assertEquals("value-0", cache(1).get(key));
   }

   private void assertPrimaryOwner(String key, int index) {
      DistributionManager dm = cache(index).getAdvancedCache().getDistributionManager();
      assertTrue(dm.getCacheTopology().getDistribution(key).isPrimary());
   }
}
