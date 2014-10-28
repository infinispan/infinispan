package org.infinispan.tx.recovery;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.infinispan.transaction.xa.TransactionXaAdapter;
import org.testng.annotations.Test;

import static org.infinispan.test.TestingUtil.existsObject;
import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.infinispan.tx.recovery.RecoveryTestUtil.*;
import static org.testng.Assert.assertEquals;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test (groups = "functional", testName = "tx.recovery.LocalRecoveryTest")
public class LocalRecoveryTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .useSynchronization(false)
            .transactionManagerLookup(new RecoveryDummyTransactionManagerLookup())
            .recovery().enable();
      return TestCacheManagerFactory.createCacheManager(cb);
   }

   public void testRecoveryManagerInJmx() throws Exception {
      assert cache.getCacheConfiguration().transaction().transactionMode().isTransactional();
      String jmxDomain = cacheManager.getCacheManagerConfiguration().globalJmxStatistics().domain();
      assert !existsObject(getCacheObjectName(jmxDomain, cache.getName(), "RecoveryManager"));
   }

   public void testOneTx() throws Exception {
      dummyTm().begin();
      cache.put("k", "v");
      TransactionXaAdapter xaRes = (TransactionXaAdapter) dummyTm().firstEnlistedResource();
      assertPrepared(0, dummyTm().getTransaction());
      xaRes.prepare(xaRes.getLocalTransaction().getXid());
      assertPrepared(1, dummyTm().getTransaction());
      final DummyTransaction suspend = (DummyTransaction) dummyTm().suspend();

      xaRes.commit(xaRes.getLocalTransaction().getXid(), false);
      assertPrepared(0, suspend);
      assertEquals(0, TestingUtil.getTransactionTable(cache).getLocalTxCount());
   }

   public void testMultipleTransactions() throws Exception {
      DummyTransaction suspend1 = beginTx();
      DummyTransaction suspend2 = beginTx();
      DummyTransaction suspend3 = beginTx();
      DummyTransaction suspend4 = beginTx();

      assertPrepared(0, suspend1, suspend2, suspend3, suspend4);

      prepareTransaction(suspend1);
      assertPrepared(1, suspend1, suspend2, suspend3, suspend4);

      prepareTransaction(suspend2);
      assertPrepared(2, suspend1, suspend2, suspend3, suspend4);

      prepareTransaction(suspend3);
      assertPrepared(3, suspend1, suspend2, suspend3, suspend4);

      prepareTransaction(suspend4);
      assertPrepared(4, suspend1, suspend2, suspend3, suspend4);

      commitTransaction(suspend1);
      assertPrepared(3, suspend1, suspend2, suspend3, suspend4);

      commitTransaction(suspend2);
      assertPrepared(2, suspend1, suspend2, suspend3, suspend4);

      commitTransaction(suspend3);
      assertPrepared(1, suspend1, suspend2, suspend3, suspend4);

      commitTransaction(suspend4);
      assertPrepared(0, suspend1, suspend2, suspend3, suspend4);

      assertEquals(0, TestingUtil.getTransactionTable(cache).getLocalTxCount());
   }

   private DummyTransaction beginTx() {
      return beginAndSuspendTx(cache);
   }


   private DummyTransactionManager dummyTm() {
      return (DummyTransactionManager) tm();
   }
}
