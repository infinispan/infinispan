package org.infinispan.tx.recovery;

import static org.infinispan.test.TestingUtil.getCacheObjectName;
import static org.infinispan.tx.recovery.RecoveryTestUtil.assertPrepared;
import static org.infinispan.tx.recovery.RecoveryTestUtil.beginAndSuspendTx;
import static org.infinispan.tx.recovery.RecoveryTestUtil.commitTransaction;
import static org.infinispan.tx.recovery.RecoveryTestUtil.prepareTransaction;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import javax.management.ObjectName;

import org.infinispan.commons.jmx.MBeanServerLookup;
import org.infinispan.commons.jmx.TestMBeanServerLookup;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.infinispan.transaction.xa.TransactionXaAdapter;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "tx.recovery.LocalRecoveryTest")
public class LocalRecoveryTest extends SingleCacheManagerTest {

   private final MBeanServerLookup mBeanServerLookup = TestMBeanServerLookup.create();

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      GlobalConfigurationBuilder gcb = new GlobalConfigurationBuilder().nonClusteredDefault();
      gcb.jmx().enabled(true).mBeanServerLookup(mBeanServerLookup);
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .useSynchronization(false)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .recovery().enable();
      return TestCacheManagerFactory.createCacheManager(gcb, cb);
   }

   public void testRecoveryManagerInJmx() {
      assertTrue(cache.getCacheConfiguration().transaction().transactionMode().isTransactional());
      String jmxDomain = cacheManager.getCacheManagerConfiguration().jmx().domain();
      ObjectName recoveryManager = getCacheObjectName(jmxDomain, cache.getName() + "(local)", "RecoveryManager");
      assertFalse(mBeanServerLookup.getMBeanServer().isRegistered(recoveryManager));
   }

   public void testOneTx() throws Exception {
      embeddedTm().begin();
      cache.put("k", "v");
      TransactionXaAdapter xaRes = (TransactionXaAdapter) embeddedTm().firstEnlistedResource();
      assertPrepared(0, embeddedTm().getTransaction());
      xaRes.prepare(xaRes.getLocalTransaction().getXid());
      assertPrepared(1, embeddedTm().getTransaction());
      final EmbeddedTransaction suspend = (EmbeddedTransaction) embeddedTm().suspend();

      xaRes.commit(xaRes.getLocalTransaction().getXid(), false);
      assertPrepared(0, suspend);
      assertEquals(0, TestingUtil.getTransactionTable(cache).getLocalTxCount());
   }

   public void testMultipleTransactions() throws Exception {
      EmbeddedTransaction suspend1 = beginTx();
      EmbeddedTransaction suspend2 = beginTx();
      EmbeddedTransaction suspend3 = beginTx();
      EmbeddedTransaction suspend4 = beginTx();

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

   private EmbeddedTransaction beginTx() {
      return beginAndSuspendTx(cache);
   }

   private EmbeddedTransactionManager embeddedTm() {
      return (EmbeddedTransactionManager) tm();
   }
}
