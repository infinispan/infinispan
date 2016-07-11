package org.infinispan.persistence.jdbc.stores;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.common.AbstractJdbcStore;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.stringbased.JdbcStringBasedStore;
import org.infinispan.persistence.jdbc.table.management.TableName;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerImpl;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.RollbackException;
import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static javax.transaction.Status.STATUS_ROLLEDBACK;

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
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table(), false);

      cacheManager = TestCacheManagerFactory.createCacheManager(cc);
      cache = cacheManager.getCache("Test");
      store = TestingUtil.getFirstTxWriter(cache);
   }

   @AfterMethod
   public void tearDown() throws PersistenceException {
      store.clear();
      assertRowCount(0);
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
      PersistenceManagerImpl pm = (PersistenceManagerImpl) TestingUtil.extractComponent(cache, PersistenceManager.class);
      pm = spy(pm);
      doThrow(new PersistenceException()).when(pm).prepareAllTxStores(any(), any(), any());
      TestingUtil.replaceComponent(cache, PersistenceManager.class, pm, true);
      TransactionManager tm = TestingUtil.getTransactionManager(cache);
      Transaction tx = null;
      try {
         tm.begin();
         tx = tm.getTransaction();
         cache.put(KEY1, VAL1);
         cache.put(KEY2, VAL2); // Throws PersistenceException, forcing the Tx to rollback
         tm.commit();
      } catch (RollbackException e) {
         // Ensure PersistenceException was the cause of the rollback
         boolean persistenceEx = false;
         Throwable[] suppressed = e.getSuppressed();
         for (Throwable ex : suppressed) {
            persistenceEx = ex.getCause() instanceof PersistenceException;
            if (persistenceEx) break;
         }
         assert persistenceEx;
      }
      assert tx != null && tx.getStatus() == STATUS_ROLLEDBACK;
      assertRowCount(0);
   }

   private void assertRowCount(int rowCount) {
      ConnectionFactory connectionFactory = store.getConnectionFactory();
      TableName tableName = store.getTableManager().getTableName();
      int value = UnitTestDatabaseManager.rowCount(connectionFactory, tableName);
      assert value == rowCount : "Expected " + rowCount + " rows, actual value is " + value;
   }
}
