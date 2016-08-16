package org.infinispan.api.batch;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "api.batch.LockInBatchTest")
public class LockInBatchTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder dccc = getDefaultClusteredCacheConfig(CacheMode.LOCAL, false);
      dccc.transaction().transactionMode(TransactionMode.TRANSACTIONAL).lockingMode(LockingMode.PESSIMISTIC);
      dccc.invocationBatching().enable(true);
      return TestCacheManagerFactory.createCacheManager(dccc);
   }

   public void testLockWithBatchingRollback() {
      cache.startBatch();
      cache.getAdvancedCache().lock("k");
      assertTrue(lockManager().isLocked("k"));
      cache().endBatch(false);
      assertFalse(lockManager().isLocked("k"));
   }

   public void testLockWithBatchingCommit() {
      cache.startBatch();
      cache.getAdvancedCache().lock("k");
      assertTrue(lockManager().isLocked("k"));
      cache().endBatch(true);
      assertFalse(lockManager().isLocked("k"));
   }

   public void testLockWithTmRollback() throws Throwable {
      tm().begin();
      cache.getAdvancedCache().lock("k");
      assertTrue(lockManager().isLocked("k"));
      tm().rollback();
      assertFalse(lockManager().isLocked("k"));
   }

   public void testLockWithTmCommit() throws Throwable {
      tm().begin();
      cache.getAdvancedCache().lock("k");
      assertTrue(lockManager().isLocked("k"));
      tm().commit();
      assertFalse(lockManager().isLocked("k"));
   }
}
