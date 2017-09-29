package org.infinispan.server.hotrod.tx;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.InvalidTransactionException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.infinispan.Cache;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.Constants;
import org.infinispan.server.hotrod.HotRodMultiNodeTest;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.server.hotrod.test.TestErrorResponse;
import org.infinispan.server.hotrod.test.TxResponse;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.lookup.TransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * Tests for valid and invalid configurations.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
@Test(groups = "functional", testName = "server.hotrod.tx.ServerConfigurationTest")
public class ServerConfigurationTest extends HotRodMultiNodeTest {

   public void testNonTransactionalConfiguration() {
      final String cacheName = "non_tx_cache";
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      doWrongConfigurationTest(cacheName, builder,
            "java.lang.IllegalStateException: ISPN006020: Cache 'non_tx_cache' is not transactional to execute a client transaction");
   }

   public void testWrongTransactionManager() {
      final String cacheName = "wrong_tm_cache";
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      builder.transaction().transactionManagerLookup(new TestTransactionManagerLookup());
      builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      doWrongConfigurationTest(cacheName, builder,
            "java.lang.IllegalStateException: ISPN006021: Cache 'wrong_tm_cache' must have EmbeddedTransactionManager as TransactionManager");
   }

   public void testWrongIsolationLevel() {
      final String cacheName = "wrong_isolation_cache";
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      builder.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      builder.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
      builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      doWrongConfigurationTest(cacheName, builder,
            "java.lang.IllegalStateException: ISPN006022: Cache 'wrong_isolation_cache' must have REPEATABLE_READ isolation level");
   }

   public void testSynchronizationMode() {
      final String cacheName = "sync_cache";
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      builder.transaction().useSynchronization(true);
      //TODO how to configure EmbeddedTransactionManagerLookup as default?
      builder.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      doCorrectConfigurationTest(cacheName, builder);
   }

   public void testXa() {
      final String cacheName = "xa_cache";
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      builder.transaction().useSynchronization(false);
      builder.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      doCorrectConfigurationTest(cacheName, builder);
   }

   public void testFullXa() {
      final String cacheName = "full_xa_cache";
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      builder.transaction().useSynchronization(false);
      builder.transaction().recovery().enable();
      builder.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      doCorrectConfigurationTest(cacheName, builder);
   }

   /*
    * TODO change when ISPN-7672 is finished!
    */
   public void testOptimisticConfiguration() {
      final String cacheName = "opt-cache";
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      builder.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      builder.transaction().lockingMode(LockingMode.OPTIMISTIC);
      doWrongConfigurationTest(cacheName, builder,
            "java.lang.IllegalStateException: Cache 'opt-cache' cannot use Optimistic neither Total Order transactions.");
   }

   /*
    * TODO change when ISPN-7672 is finished!
    */
   public void testTotalOrderConfiguration() {
      final String cacheName = "total-order-cache";
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      builder.transaction().transactionManagerLookup(new EmbeddedTransactionManagerLookup());
      builder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER);
      builder.clustering().cacheMode(CacheMode.REPL_SYNC);
      doWrongConfigurationTest(cacheName, builder,
            "java.lang.IllegalStateException: Cache 'total-order-cache' cannot use Optimistic neither Total Order transactions.");
   }

   @Override
   protected String cacheName() {
      return "default";
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      return new ConfigurationBuilder();
   }

   @Override
   protected byte protocolVersion() {
      return Constants.VERSION_27;
   }

   private void doWrongConfigurationTest(String cacheName, ConfigurationBuilder builder, String errorMsg) {
      cacheManagers.forEach(cm -> cm.defineConfiguration(cacheName, builder.build()));
      waitForClusterToForm(cacheName);
      HotRodClient client = createClient(cacheName);
      XidImpl xid = XidImpl.create(-1, new byte[]{2}, new byte[]{3});
      try {
         TestErrorResponse response = (TestErrorResponse) client.prepareTx(xid, false, Collections.emptyList());
         assertEquals(errorMsg, response.msg);
         response = (TestErrorResponse) client.commitTx(xid);
         assertEquals(errorMsg, response.msg);
         response = (TestErrorResponse) client.rollbackTx(xid);
         assertEquals(errorMsg, response.msg);
         assertServerTransactionTableEmpty(cacheName);
      } finally {
         HotRodTestingUtil.killClient(client);
      }
   }

   private void doCorrectConfigurationTest(String cacheName, ConfigurationBuilder builder) {
      cacheManagers.forEach(cm -> cm.defineConfiguration(cacheName, builder.build()));
      waitForClusterToForm(cacheName);
      HotRodClient client = createClient(cacheName);
      XidImpl xid = XidImpl.create(-1, new byte[]{2}, new byte[]{3});
      try {
         TxResponse response = (TxResponse) client.prepareTx(xid, false, Collections.emptyList());
         assertEquals(XAResource.XA_RDONLY, response.xaCode);
         response = (TxResponse) client.commitTx(xid);
         assertEquals(XAResource.XA_OK, response.xaCode);
         response = (TxResponse) client.rollbackTx(xid);
         assertEquals(XAResource.XA_OK, response.xaCode);
         assertServerTransactionTableEmpty(cacheName);
      } finally {
         HotRodTestingUtil.killClient(client);
      }
   }

   private void assertServerTransactionTableEmpty(String cacheName) {
      for (Cache cache : caches(cacheName)) {
         if (cache.getCacheConfiguration().transaction().transactionMode().isTransactional()) {
            ServerTransactionTable serverTransactionTable = extractComponent(cache, ServerTransactionTable.class);
            assertTrue(serverTransactionTable.isEmpty());
         }
      }
   }

   private HotRodClient createClient(String cacheName) {
      return new HotRodClient("127.0.0.1", servers().get(0).getPort(), cacheName, 60, protocolVersion());
   }

   private static class TestTransactionManagerLookup implements TransactionManagerLookup {

      @Override
      public TransactionManager getTransactionManager() throws Exception {
         return new TransactionManager() {
            @Override
            public void begin() throws NotSupportedException, SystemException {

            }

            @Override
            public void commit()
                  throws RollbackException, HeuristicMixedException, HeuristicRollbackException, SecurityException,
                  IllegalStateException, SystemException {

            }

            @Override
            public void rollback() throws IllegalStateException, SecurityException, SystemException {

            }

            @Override
            public void setRollbackOnly() throws IllegalStateException, SystemException {

            }

            @Override
            public int getStatus() throws SystemException {
               return 0;
            }

            @Override
            public Transaction getTransaction() throws SystemException {
               return null;
            }

            @Override
            public void setTransactionTimeout(int seconds) throws SystemException {

            }

            @Override
            public Transaction suspend() throws SystemException {
               return null;
            }

            @Override
            public void resume(Transaction tobj)
                  throws InvalidTransactionException, IllegalStateException, SystemException {

            }
         };
      }
   }
}
