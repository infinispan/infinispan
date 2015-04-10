package org.infinispan.tx.exception;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.LockManager;
import org.testng.annotations.Test;

import javax.transaction.Status;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Collections;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

/**
 * Tester for https://jira.jboss.org/browse/ISPN-629.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(testName = "tx.exception.TxAndTimeoutExceptionTest", groups = "functional")
public class TxAndTimeoutExceptionTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder config = getDefaultStandaloneCacheConfig(true);
      config
         .transaction().lockingMode(LockingMode.PESSIMISTIC)
         .locking().useLockStriping(false).lockAcquisitionTimeout(1000);
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(config);
      cache = cm.getCache();
      return cm;
   }


   public void testPutTimeoutsInTx() throws Exception {
      assertExpectedBehavior(new CacheOperation() {
         @Override
         public void execute() {
            cache.put("k1", "v2222");
         }
      });
   }

   public void testRemoveTimeoutsInTx() throws Exception {
      assertExpectedBehavior(new CacheOperation() {
         @Override
         public void execute() {
            cache.remove("k1");
         }
      });
   }

   public void testReplaceTimeoutsInTx() throws Exception {
      assertExpectedBehavior(new CacheOperation() {
         @Override
         public void execute() {
            cache.replace("k1", "newValue");
         }
      });
   }

   public void testPutAllTimeoutsInTx() throws Exception {
      assertExpectedBehavior(new CacheOperation() {
         @Override
         public void execute() {
            cache.putAll(Collections.singletonMap("k1", "v22222"));
         }
      });
   }

   private void assertExpectedBehavior(CacheOperation op) throws Exception {
      LockManager lm = TestingUtil.extractLockManager(cache);
      TransactionTable txTable = TestingUtil.getTransactionTable(cache);
      TransactionManager tm = cache.getAdvancedCache().getTransactionManager();
      tm.begin();
      cache.put("k1", "v1");
      Transaction k1LockOwner = tm.suspend();
      assertTrue(lm.isLocked("k1"));

      assertEquals(1, txTable.getLocalTxCount());
      tm.begin();
      cache.put("k2", "v2");
      assertTrue(lm.isLocked("k2"));
      assertEquals(2, txTable.getLocalTxCount());
      assert tm.getTransaction() != null;
      try {
         op.execute();
         fail("TimeoutException expected.");
      } catch (TimeoutException e) {
         //expected
      }

      //make sure that locks acquired by that tx were released even before the transaction is rolled back, the tx object
      //was marked for rollback
      Transaction transaction = tm.getTransaction();
      assertNotNull(transaction);
      assertEquals(Status.STATUS_MARKED_ROLLBACK, transaction.getStatus());
      assertFalse(lm.isLocked("k2"));
      assertTrue(lm.isLocked("k1"));
      try {
         cache.put("k3", "v3");
         fail("IllegalStateException expected.");
      } catch (IllegalStateException e) {
         //expected
      }
      assertEquals(2, txTable.getLocalTxCount());

      //now the TM is expected to rollback the tx
      tm.rollback();
      assertEquals(1, txTable.getLocalTxCount());

      tm.resume(k1LockOwner);
      tm.commit();

      //now test that the other tx works as expected
      assertEquals(0, txTable.getLocalTxCount());
      assertEquals("v1", cache.get("k1"));
      assertFalse(lm.isLocked("k1"));
      assertEquals(0, txTable.getLocalTxCount());
   }

   public interface CacheOperation {
      void execute();
   }
}
