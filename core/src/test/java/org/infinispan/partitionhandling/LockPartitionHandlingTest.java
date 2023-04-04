package org.infinispan.partitionhandling;

import static org.infinispan.test.TestingUtil.waitForNoRebalance;
import static org.testng.AssertJUnit.assertTrue;

import java.util.stream.Stream;

import jakarta.transaction.TransactionManager;

import org.infinispan.Cache;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.MagicKey;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "partitionhandling.LockPartitionHandlingTest")
public class LockPartitionHandlingTest extends BasePartitionHandlingTest {

   public LockPartitionHandlingTest() {
      super();
      transactional = true;
      lockingMode = LockingMode.PESSIMISTIC;
   }

   public void testLockWhenDegraded() throws Exception {
      Cache<MagicKey, String> c0 = cache(0);
      MagicKey key = new MagicKey("key1", cache(1), cache(2));
      c0.put(key, "value");
      c0.put(new MagicKey("key2", cache(2), cache(0)), "value");

      splitCluster(new int[]{0, 1}, new int[]{2, 3});
      partition(0).assertDegradedMode();

      TransactionManager tm = c0.getAdvancedCache().getTransactionManager();
      tm.begin();
      Exceptions.expectException(AvailabilityException.class, () -> c0.getAdvancedCache().lock(key));
      tm.rollback();
   }

   public void testLockSucceedWhenLocal() throws Exception {
      Cache<MagicKey, String> c0 = cache(0);
      MagicKey key = new MagicKey("key1", c0, cache(1));
      c0.put(key, "value");
      c0.put(new MagicKey("key2", cache(2), cache(0)), "value");

      splitCluster(new int[]{0, 1}, new int[]{2, 3});
      partition(0).assertDegradedMode();

      TransactionManager tm = c0.getAdvancedCache().getTransactionManager();
      tm.begin();
      assertTrue(c0.getAdvancedCache().withFlags(Flag.CACHE_MODE_LOCAL).lock(key));
      tm.rollback();
   }

   public void testLockSucceedWhenAllMembersInPartition() throws Exception {
      Cache<MagicKey, String> c0 = cache(0);
      Cache<MagicKey, String> c1 = cache(1);
      MagicKey local = new MagicKey("key1", c0, c1);
      MagicKey remote = new MagicKey("key2", cache(2), cache(3));

      c0.put(local, "value");
      c0.put(remote, "value");

      splitCluster(new int[]{0, 1}, new int[]{2, 3});
      partition(0).assertDegradedMode();

      TransactionManager tm = c0.getAdvancedCache().getTransactionManager();
      tm.begin();
      assertTrue(c1.getAdvancedCache().lock(local));

      // Can not reach remote key.
      Exceptions.expectException(AvailabilityException.class, () -> c0.getAdvancedCache().lock(remote));
      Exceptions.expectException(AvailabilityException.class, () -> c1.getAdvancedCache().lock(remote));
      tm.rollback();
   }

   public void testLockWhenSplitThenMerge() throws Exception {
      Cache<MagicKey, String> c0 = cache(0);
      MagicKey key1 = new MagicKey("key1", cache(1), cache(2));
      MagicKey key2 = new MagicKey("key2", cache(2), cache(0));
      c0.put(key1, "value");
      c0.put(key2, "value");

      splitCluster(new int[]{0, 1}, new int[]{2, 3});
      partition(0).assertDegradedMode();

      TransactionManager tm = c0.getAdvancedCache().getTransactionManager();
      tm.begin();
      Exceptions.expectException(AvailabilityException.class, () -> c0.getAdvancedCache().lock(key1));
      Exceptions.expectException(AvailabilityException.class, () -> c0.getAdvancedCache().lock(key2));
      tm.rollback();

      mergeCluster();

      tm.begin();
      assertTrue(c0.getAdvancedCache().lock(key1));
      tm.rollback();
   }

   void mergeCluster() {
      partition(0).merge(partition(1));
      waitForNoRebalance(caches());
      for (int i = 0; i < numMembersInCluster; i++) {
         PartitionHandlingManager phmI = partitionHandlingManager(cache(i));
         eventuallyEquals(AvailabilityMode.AVAILABLE, phmI::getAvailabilityMode);
      }
   }

   @Override
   protected void customizeCacheConfiguration(ConfigurationBuilder dcc) {
      dcc.transaction().lockingMode(lockingMode)
            .cacheStopTimeout(0)
            .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());
   }

   @Override
   protected ConfigurationBuilder cacheConfiguration() {
      return getDefaultClusteredCacheConfig(cacheMode, transactional);
   }

   @Override
   public Object[] factory() {
      return Stream.of(PartitionHandling.values())
            .filter(ph -> ph != PartitionHandling.ALLOW_READ_WRITES)
            .map(ph -> new LockPartitionHandlingTest().partitionHandling(ph))
            .toArray();
   }
}
