package org.infinispan.tx.exception;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.fail;

import java.util.Collections;

import jakarta.transaction.HeuristicMixedException;
import jakarta.transaction.HeuristicRollbackException;
import jakarta.transaction.InvalidTransactionException;
import jakarta.transaction.NotSupportedException;
import jakarta.transaction.RollbackException;
import jakarta.transaction.SystemException;
import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * Tester for https://jira.jboss.org/browse/ISPN-629.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.2
 */
@Test(groups = "functional", testName = "tx.exception.TxAndRemoteTimeoutExceptionTest")
public class TxAndRemoteTimeoutExceptionTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(TxAndRemoteTimeoutExceptionTest.class);

   private LockManager lm1;
   private LockManager lm0;
   private TransactionTable txTable0;
   private TransactionTable txTable1;
   private TransactionManager tm;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfig = getDefaultConfig();
      defaultConfig.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis())
            .useLockStriping(false);
      addClusterEnabledCacheManager(defaultConfig);
      addClusterEnabledCacheManager(defaultConfig);
      lm0 = TestingUtil.extractLockManager(cache(0));
      lm1 = TestingUtil.extractLockManager(cache(1));
      txTable0 = TestingUtil.getTransactionTable(cache(0));
      txTable1 = TestingUtil.getTransactionTable(cache(1));
      tm = cache(0).getAdvancedCache().getTransactionManager();
      TestingUtil.blockUntilViewReceived(cache(0), 2);
   }

   protected ConfigurationBuilder getDefaultConfig() {
      return getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
   }

   public void testPutTimeoutsInTx() throws Exception {
      runAssertion(() -> cache(0).put("k1", "v2222"));
   }

   public void testRemoveTimeoutsInTx() throws Exception {
      runAssertion(() -> cache(0).remove("k1"));
   }

   public void testReplaceTimeoutsInTx() throws Exception {
      cache(1).put("k1", "value");
      runAssertion(() -> cache(0).replace("k1", "newValue"));
   }

   public void testPutAllTimeoutsInTx() throws Exception {
      runAssertion(() -> cache(0).putAll(Collections.singletonMap("k1", "v22222")));
   }


   private void runAssertion(CacheOperation operation) throws NotSupportedException, SystemException, HeuristicMixedException, HeuristicRollbackException, InvalidTransactionException, RollbackException {
      // Start tx1
      tm.begin();
      cache(1).put("k1", "v1");
      EmbeddedTransaction k1LockOwner = (EmbeddedTransaction) tm.suspend();
      assertFalse(lm1.isLocked("k1"));
      assertEquals(1, txTable1.getLocalTxCount());

      // Start tx2
      tm.begin();
      cache(0).put("k2", "v2");
      assertFalse(lm0.isLocked("k2"));
      assertFalse(lm1.isLocked("k2"));

      operation.execute();

      assertEquals(1, txTable1.getLocalTxCount());
      assertEquals(1, txTable0.getLocalTxCount());

      final Transaction tx2 = tm.suspend();

      // Acquire k1 for tx1
      tm.resume(k1LockOwner);
      k1LockOwner.runPrepare();
      tm.suspend();

      // Try (and fail) to commit tx2
      tm.resume(tx2);
      try {
         tm.commit();
         fail("Rollback expected.");
      } catch (RollbackException re) {
         //expected
      }

      assertEquals(0, txTable0.getLocalTxCount());
      assertEquals(1, txTable1.getLocalTxCount());

      // Commit tx1
      log.trace("Right before second commit");
      tm.resume(k1LockOwner);
      k1LockOwner.runCommit(false);

      // Final checks
      assertEquals("v1", cache(0).get("k1"));
      assertEquals("v1", cache(1).get("k1"));
      assertEquals(0, txTable1.getLocalTxCount());
      assertEquals(0, txTable1.getLocalTxCount());
      assertNotLocked("k1");
      assertNotLocked("k2");
   }

   public interface CacheOperation {
      void execute();
   }
}
