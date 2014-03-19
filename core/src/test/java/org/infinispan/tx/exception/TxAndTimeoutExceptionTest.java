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

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertEquals;

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

   public void testClearTimeoutsInTx() throws Exception {
      cache.put("k1", "value");
      assertExpectedBehavior(new CacheOperation() {
         @Override
         public void execute() {
            cache.clear();
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
            Map toAdd = new HashMap();
            toAdd.put("k1", "v22222");
            cache.putAll(toAdd);
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
      assert lm.isLocked("k1");

      assertEquals(1, txTable.getLocalTxCount());
      tm.begin();
      cache.put("k2", "v2");
      assert lm.isLocked("k2");
      assertEquals(2, txTable.getLocalTxCount());
      assert tm.getTransaction() != null;
      try {
         op.execute();
         assert false : "Timeout exception expected";
      } catch (TimeoutException e) {
         //expected
      }

      //make sure that locks acquired by that tx were released even before the transaction is rolled back, the tx object
      //was marked for rollback
      Transaction transaction = tm.getTransaction();
      assert transaction != null;
      assert transaction.getStatus() == Status.STATUS_MARKED_ROLLBACK;
      assert !lm.isLocked("k2");
      assert lm.isLocked("k1");
      try {
         cache.put("k3", "v3");
         assert false;
      } catch (IllegalStateException e) {
         //expected
      }
      assertEquals(txTable.getLocalTxCount(), 2);

      //now the TM is expected to rollback the tx
      tm.rollback();
      assertEquals(txTable.getLocalTxCount(), 1);

      tm.resume(k1LockOwner);
      tm.commit();

      //now test that the other tx works as expected
      assertEquals(0, txTable.getLocalTxCount());
      assertEquals(cache.get("k1"), "v1");
      assert !lm.isLocked("k1");
      assertEquals(txTable.getLocalTxCount(), 0);
   }

   public interface CacheOperation {

      public abstract void execute();
   }
}
