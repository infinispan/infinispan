package org.infinispan.tx.locking;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "tx.locking.LocalOptimisticTxTest")
public class LocalOptimisticTxTest extends AbstractLocalTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      final ConfigurationBuilder config = getDefaultStandaloneCacheConfig(true);
      config
         .transaction()
            .lockingMode(LockingMode.OPTIMISTIC)
            .transactionManagerLookup(new DummyTransactionManagerLookup())
            .useSynchronization(false)
            .recovery()
               .disable();
      return TestCacheManagerFactory.createCacheManager(config);
   }

   @Override
   protected void assertLockingOnRollback() {
      assertFalse(lockManager().isLocked("k"));
      rollback();
      assertFalse(lockManager().isLocked("k"));
   }

   @Override
   protected void assertLocking() {
      assertFalse(lockManager().isLocked("k"));
      prepare();
      assertTrue(lockManager().isLocked("k"));
      commit();
      assertFalse(lockManager().isLocked("k"));
   }
}
