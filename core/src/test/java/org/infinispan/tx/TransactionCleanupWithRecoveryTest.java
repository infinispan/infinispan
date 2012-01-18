package org.infinispan.tx;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.tx.recovery.RecoveryDummyTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;


import javax.transaction.Transaction;

import static org.testng.Assert.assertEquals;

@Test(groups = "functional", testName = "TransactionCleanupWithRecoveryTest")
public class TransactionCleanupWithRecoveryTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      Configuration cfg = new Configuration().fluent()
            .clustering().mode(Configuration.CacheMode.REPL_SYNC)
            .locking()
            .concurrencyLevel(10000).isolationLevel(IsolationLevel.REPEATABLE_READ)
            .lockAcquisitionTimeout(100L).useLockStriping(false).writeSkewCheck(true)
            .transaction()
            .lockingMode(LockingMode.PESSIMISTIC)
            .recovery()
            .transactionManagerLookup(new RecoveryDummyTransactionManagerLookup())
            .build();

      registerCacheManager(TestCacheManagerFactory.createClusteredCacheManager(cfg),
                           TestCacheManagerFactory.createClusteredCacheManager(cfg));
   }

   public void testCleanup() throws Exception {

      cache(0).put(1, "v1");

      assertNoTx();
   }

   public void testWithSilentFailure() throws Exception {
      Cache<Integer, String> c0 = cache(0), c1 = cache(1);

      c0.put(1, "v1");
      assertNoTx();

      tm(1).begin();
      c1.put(1, "v2");
      Transaction suspendedTx = tm(1).suspend();

      try {
         Cache<Integer, String> silentC0 = c0.getAdvancedCache().withFlags(
               Flag.FAIL_SILENTLY, Flag.ZERO_LOCK_ACQUISITION_TIMEOUT);

         tm(0).begin();
         assert !silentC0.getAdvancedCache().lock(1);
         assert "v1".equals(silentC0.get(1));
         tm(0).rollback();
      } finally {
         tm(1).resume(suspendedTx);
         tm(1).commit();
      }

      assertNoTx();
   }

   private void assertNoTx() {
      final TransactionTable tt0 = TestingUtil.getTransactionTable(cache(0));
      assertEquals(tt0.getLocalTxCount(), 0);
      assertEquals(tt0.getRemoteTxCount(), 0);

      final TransactionTable tt1 = TestingUtil.getTransactionTable(cache(1));
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            int localTxCount = tt1.getLocalTxCount();
            int remoteTxCount = tt1.getRemoteTxCount();
            return localTxCount == 0 && remoteTxCount == 0;
         }
      });
   }
}