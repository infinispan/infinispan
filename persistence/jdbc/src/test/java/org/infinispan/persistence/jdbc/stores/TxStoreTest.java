package org.infinispan.persistence.jdbc.stores;

import static javax.transaction.Status.STATUS_ROLLEDBACK;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.testng.AssertJUnit.assertEquals;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAException;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.table.TableName;
import org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStore;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.Exceptions;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Test to check that a transactional store commits/rollsback stored values as expected. Also ensures that a failed
 * prepare on the store results in the entire cache Tx rollingback.
 *
 * @author Ryan Emerson
 */
@Test(groups = "functional", testName = "persistence.jdbc.stores.TxStoreTest")
public class TxStoreTest extends AbstractInfinispanTest {

   private static final String KEY1 = "Key 1";
   private static final String KEY2 = "Key 2";
   private static final String VAL1 = "Val 1";
   private static final String VAL2 = "Val 2";

   private EmbeddedCacheManager cacheManager;
   private Cache<String, String> cache;
   private JdbcStringBasedStore store;

   @BeforeMethod
   public void beforeClass() {
      ConfigurationBuilder cc = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = cc
            .persistence()
               .addStore(JdbcStringBasedStoreConfigurationBuilder.class)
               .shared(true)
               .transactional(true);

      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      UnitTestDatabaseManager.setDialect(storeBuilder);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table());

      cacheManager = TestCacheManagerFactory.createCacheManager(new GlobalConfigurationBuilder().defaultCacheName("Test"), cc);
      cache = cacheManager.getCache("Test");
      store = TestingUtil.getFirstTxWriter(cache);
   }

   @AfterMethod(alwaysRun = true)
   public void tearDown() throws PersistenceException {
      if (store != null) {
         store.clear();
         assertRowCount(0);
      }
      TestingUtil.killCacheManagers(cacheManager);
   }

   @Test
   public void testTxCommit() throws Exception {
      cache.put(KEY1, VAL1);
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      tm.begin();
      cache.put(KEY2, VAL1);
      String oldValue = cache.put(KEY1, VAL2);
      assertEquals(oldValue, VAL1);
      tm.commit();

      String cacheVal = cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).get(KEY1);
      assertEquals(cacheVal, VAL2);

      // Ensure the values committed in the Tx were actually written to the store as well as to the cache
      assertRowCount(2);
      assertEquals(store.load(KEY1).getValue(), VAL2);
      assertEquals(store.load(KEY2).getValue(), VAL1);
   }

   @Test
   public void testTxRollback() throws Exception {
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      tm.begin();
      Transaction tx = tm.getTransaction();
      cache.put(KEY1, VAL1);
      cache.put(KEY2, VAL2);
      tm.rollback();
      assert tx.getStatus() == STATUS_ROLLEDBACK;
      assertRowCount(0);
   }

   @Test
   public void testTxRollbackOnStoreException() throws Exception {
      PersistenceManager pm = TestingUtil.extractComponent(cache, PersistenceManager.class);
      PersistenceManager mockPM = mock(PersistenceManager.class);
      doThrow(new PersistenceException()).when(mockPM).prepareAllTxStores(any(), any(), any());
      TestingUtil.replaceComponent(cache, PersistenceManager.class, mockPM, true);

      try {
         TransactionManager tm = TestingUtil.getTransactionManager(cache);
         tm.begin();
         Transaction tx = tm.getTransaction();
         cache.put(KEY1, VAL1);
         cache.put(KEY2, VAL2);
         // Throws PersistenceException, forcing the Tx to rollback
         Throwable throwable = Exceptions.extractException(tm::commit);
         Exceptions.assertException(RollbackException.class, throwable);
         Exceptions.assertException(XAException.class, PersistenceException.class, throwable.getSuppressed()[0]);
         assertEquals(STATUS_ROLLEDBACK, tx.getStatus());
         assertRowCount(0);
      } finally {
         // The mock doesn't have any metadata, so its stop() method won't be invoked
         pm.stop();
         store = null;
      }
   }

   private void assertRowCount(int rowCount) {
      ConnectionFactory connectionFactory = store.getConnectionFactory();
      TableName tableName = store.getTableManager().getTableName();
      int value = UnitTestDatabaseManager.rowCount(connectionFactory, tableName);
      assert value == rowCount : "Expected " + rowCount + " rows, actual value is " + value;
   }

   public void testSizeWithEntryInContext() throws Exception {
      cache.put(KEY1, VAL1);

      assertEquals(1, cache.size());

      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      TestingUtil.withTx(tm, () -> {
         cache.put(KEY2, VAL2);
         assertEquals(2, cache.size());
         return null;
      });
   }
}
