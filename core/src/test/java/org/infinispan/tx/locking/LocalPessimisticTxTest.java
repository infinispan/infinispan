package org.infinispan.tx.locking;

import org.infinispan.config.Configuration;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "tx.locking.LocalPessimisticTxTest")
public class LocalPessimisticTxTest extends AbstractLocalTest {

   public void testLockingWithRollback() throws Exception {
      tm().begin();
      cache().getAdvancedCache().lock("k");
      assertLockingOnRollback();
      assertNull(cache().get("k"));

      tm().begin();
      cache().getAdvancedCache().lock("k");
      cache().put("k", "v");
      assertLockingOnRollback();
      assertNull(cache().get("k"));
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      final Configuration config = getDefaultStandaloneConfig(true);
      config.fluent().transaction().lockingMode(LockingMode.PESSIMISTIC)
            .transactionManagerLookup(new DummyTransactionManagerLookup());
      return TestCacheManagerFactory.createCacheManager(config);
   }

   @Override
   protected void assertLockingOnRollback() {
      assertTrue(lockManager().isLocked("k"));
      rollback();
      assertFalse(lockManager().isLocked("k"));
   }

   @Override
   protected void assertLocking() {
      assertTrue(lockManager().isLocked("k"));
      commit();
      assertFalse(lockManager().isLocked("k"));
   }
}
