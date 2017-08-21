package org.infinispan.lock.singlelock;

import static org.testng.Assert.assertEquals;

import java.util.Collections;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.eventually.Eventually;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test(groups = "functional")
public abstract class AbstractNoCrashTest extends MultipleCacheManagersTest {

   protected CacheMode cacheMode;
   protected LockingMode lockingMode;
   protected Boolean useSynchronization;

   protected AbstractNoCrashTest(CacheMode cacheMode, LockingMode lockingMode, Boolean useSynchronization) {
      this.cacheMode = cacheMode;
      this.lockingMode = lockingMode;
      this.useSynchronization = useSynchronization;
   }

   @Override
   protected final void createCacheManagers() {
      assert cacheMode != null && lockingMode != null && useSynchronization != null;
      ConfigurationBuilder config = getDefaultClusteredCacheConfig(cacheMode, true);
      config.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .lockingMode(lockingMode)
            .useSynchronization(useSynchronization);
      config.clustering().hash().numOwners(3)
            .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());
      createCluster(config, 3);
      waitForClusterToForm();
   }

   protected interface Operation {
      void perform(Object key, int cacheIndex);
   }

   public void testTxAndLockOnDifferentNodesPut() throws Exception {
      testTxAndLockOnDifferentNodes((key, cacheIndex) -> cache(cacheIndex).put(key, "v"), false, false);
   }

   public void testTxAndLockOnDifferentNodesPutAll() throws Exception {
      testTxAndLockOnDifferentNodes((key, cacheIndex) -> cache(cacheIndex).putAll(Collections.singletonMap(key, "v")), false, false);
   }

   public void testTxAndLockOnDifferentNodesReplace() throws Exception {
      testTxAndLockOnDifferentNodes((key, cacheIndex) -> cache(cacheIndex).replace(key, "v"), true, false);
   }

   public void testTxAndLockOnDifferentNodesRemove() throws Exception {
      testTxAndLockOnDifferentNodes((key, cacheIndex) -> cache(cacheIndex).remove(key), true, true);
   }

   public void testTxAndLockOnSameNode() throws Exception {
      final Object k = getKeyForCache(0);

      tm(0).begin();
      cache(0).put(k, "v");
      EmbeddedTransaction dtm = (EmbeddedTransaction) tm(0).getTransaction();

      dtm.runPrepare();

      assert lockManager(0).isLocked(k);
      assert !lockManager(1).isLocked(k);
      assert !lockManager(2).isLocked(k);

      dtm.runCommit(false);

      assertNotLocked(k);
      assertValue(k, false);
   }

   protected abstract void testTxAndLockOnDifferentNodes(Operation operation, boolean addFirst, boolean removed) throws Exception;

   protected boolean noPendingTransactions(int i) {
      return checkTxCount(i, 0, 0);
   }

   protected void assertValue(Object k, boolean isRemove) {
      Eventually.eventually(() -> noPendingTransactions(0) && noPendingTransactions(1) && noPendingTransactions(2));
      final String expected = isRemove ? null : "v";
      assertEquals(cache(0).get(k), expected);
      Eventually.eventually(() -> noPendingTransactions(0) && noPendingTransactions(1) && noPendingTransactions(2));


      assertEquals(cache(1).get(k), expected);
      assertEquals(cache(2).get(k), expected);
   }
}
