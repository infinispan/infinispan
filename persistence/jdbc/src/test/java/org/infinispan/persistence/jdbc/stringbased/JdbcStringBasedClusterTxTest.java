package org.infinispan.persistence.jdbc.stringbased;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commons.test.skip.SkipTestNG;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.distribution.BaseDistStoreTest;
import org.infinispan.distribution.MagicKey;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.UnitTestDatabaseManager;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.GenericTransactionManagerLookup;
import org.testng.annotations.Test;

import jakarta.transaction.TransactionManager;

@Test(groups = "functional", testName = "persistence.jdbc.JdbcStringBasedClusterTxTest")
public class JdbcStringBasedClusterTxTest extends BaseDistStoreTest<Integer, String, JdbcStringBasedClusterTxTest> {

   {
      INIT_CLUSTER_SIZE = 2;
      l1CacheEnabled = false;
   }

   private boolean useSynchronization;
   private boolean useAutoCommit;

   @Override
   public Object[] factory() {
      return Stream.of(Boolean.TRUE, Boolean.FALSE)
            .flatMap(segmented ->
                  Stream.of(LockingMode.values())
                        .flatMap(lm ->
                              Stream.of(Boolean.TRUE, Boolean.FALSE)
                                    .flatMap(autoCommit ->
                                          Stream.of(Boolean.TRUE, Boolean.FALSE)
                                                .flatMap(synchronization ->
                                                            Stream.of(
                                                                  new JdbcStringBasedClusterTxTest()
                                                                        .withAutoCommit(autoCommit)
                                                                        .withSynchronization(synchronization)
                                                                        .segmented(segmented)
                                                                        .shared(true)
                                                                        .cacheMode(CacheMode.REPL_SYNC)
                                                                        .transactional(true)
                                                                        .lockingMode(lm),
                                                                  new JdbcStringBasedClusterTxTest()
                                                                        .withAutoCommit(autoCommit)
                                                                        .withSynchronization(synchronization)
                                                                        .segmented(segmented)
                                                                        .shared(true)
                                                                        .cacheMode(CacheMode.DIST_SYNC)
                                                                        .transactional(true)
                                                                        .lockingMode(lm)
                                                            )
                                                )
                                    )
                        )
            )
            .toArray(Object[]::new);
   }

   @Override
   protected String[] parameterNames() {
      return concat(super.parameterNames(), "synchronization", "auto-commit");
   }

   @Override
   protected Object[] parameterValues() {
      return concat(super.parameterValues(), useSynchronization, useAutoCommit);
   }

   private JdbcStringBasedClusterTxTest withAutoCommit(boolean useAutoCommit) {
      this.useAutoCommit = useAutoCommit;
      return this;
   }

   private JdbcStringBasedClusterTxTest withSynchronization(boolean useSynchronization) {
      this.useSynchronization = useSynchronization;
      return this;
   }

   @Override
   protected StoreConfigurationBuilder addStore(PersistenceConfigurationBuilder persistenceConfigurationBuilder, boolean shared) {
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = persistenceConfigurationBuilder
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      storeBuilder.shared(shared);
      storeBuilder.segmented(segmented);
      // Ensure more than one to trigger backup requests, too.
      storeBuilder.clustering().hash().numOwners(2);

      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);

      storeBuilder
            .locking()
            .isolationLevel(IsolationLevel.REPEATABLE_READ);

      storeBuilder
            .transactional(true)
            .transaction()
            .lockingMode(lockingMode)
            .autoCommit(useAutoCommit)
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .useSynchronization(useSynchronization)
            .transactionManagerLookup(new GenericTransactionManagerLookup());
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table());

      return storeBuilder;
   }

   public void testWriteCount() {
      SkipTestNG.skipIf(!useAutoCommit, "requires auto-commit to be true");
      var builder = buildConfiguration();
      builder.clustering().hash().numOwners(2);
      builder.persistence().clearStores();
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName("test-write-count")
            .shared(true);
      manager(0).defineConfiguration("write-count", builder.build());
      manager(1).defineConfiguration("write-count", builder.build());
      TestingUtil.waitForNoRebalance(caches("write-count"));

      DummyInMemoryStore store = TestingUtil.getFirstStore(cache(0, "write-count"));

      cache(0, "write-count").put(new MagicKey("key", cache(1, "write-count")), "value");

      assertEquals(1, (int) store.stats().get("write"));
   }

   public void testElementsInStore() throws Throwable {
      int dataSize = 1;
      Cache<String, String> cacheOne = cache(0, cacheName);
      Cache<String, String> cacheTwo = cache(1, cacheName);

      PersistenceManager persistenceManager = TestingUtil.extractComponent(cache(0, cacheName), PersistenceManager.class);
      JdbcStringBasedStore jdbcStringBasedStore = persistenceManager.getStores(JdbcStringBasedStore.class).iterator().next();
      ConnectionFactory connectionFactory = jdbcStringBasedStore.getConnectionFactory();
      String tableName = jdbcStringBasedStore.getTableManager().getDataTableName().toString();
      Connection connection = connectionFactory.getConnection();

      Set<String> keys = new HashSet<>(dataSize);

      try {
         for (int i = 0; i < dataSize; i++) {
            String key = getStringKeyForCache(cacheOne);
            String value = "value-" + i;
            performCacheOperation(cacheOne, () -> cacheOne.put(key, value));
            assertEntryInDatabase(connection, key, tableName, true);
            keys.add(key);
         }
      } finally {
         if (connection != null)
            connectionFactory.releaseConnection(connection);
      }

      // Ensure both nodes view the same data.
      assertEquals(dataSize, cacheOne.size());
      assertEquals(dataSize, cacheTwo.size());

      // Ensure the database has all the data.
      assertEquals(dataSize, UnitTestDatabaseManager.rowCount(connectionFactory, jdbcStringBasedStore.getTableManager().getDataTableName()));

      // Now perform remove operations on every key.
      for (String key : keys) {
         performCacheOperation(cacheOne, () -> cacheOne.remove(key));

         connection = connectionFactory.getConnection();
         try {
            assertEntryInDatabase(connection, key, tableName, false);
         } finally {
            if (connection != null)
               connectionFactory.releaseConnection(connection);
            connection = null;
         }
      }

      // Ensure everything is empty now.
      assertEquals(0, cacheOne.size());
      assertEquals(0, cacheTwo.size());
      assertEquals(0, UnitTestDatabaseManager.rowCount(connectionFactory, jdbcStringBasedStore.getTableManager().getDataTableName()));
   }

   public void testCommitManyElements() throws Throwable {
      int dataSize = 10;
      Cache<String, String> cacheOne = cache(0, cacheName);
      Cache<String, String> cacheTwo = cache(1, cacheName);

      PersistenceManager persistenceManager = TestingUtil.extractComponent(cache(0, cacheName), PersistenceManager.class);
      JdbcStringBasedStore jdbcStringBasedStore = persistenceManager.getStores(JdbcStringBasedStore.class).iterator().next();
      ConnectionFactory connectionFactory = jdbcStringBasedStore.getConnectionFactory();

      Map<String, String> batch = new HashMap<>();
      for (int i = 0; i < dataSize; i++) {
         String key = getStringKeyForCache((i & 1) == 1 ? cacheOne : cacheTwo);
         batch.put(key, "value-" + i);
      }

      performCacheOperation(cacheOne, () -> cacheOne.putAll(batch));

      // Ensure both nodes view the same data.
      assertEquals(dataSize, cacheOne.size());
      assertEquals(dataSize, cacheTwo.size());

      // Ensure the database has all the data.
      assertEquals(dataSize, UnitTestDatabaseManager.rowCount(connectionFactory, jdbcStringBasedStore.getTableManager().getDataTableName()));

      // Now verify when removing everything from the cache. It should be empty.
      performCacheOperation(cacheOne, cacheOne::clear);
      assertEquals(0, cacheOne.size());
      assertEquals(0, cacheTwo.size());
      assertEquals(0, UnitTestDatabaseManager.rowCount(connectionFactory, jdbcStringBasedStore.getTableManager().getDataTableName()));
   }

   public void testManyElementsRollback() throws Throwable {
      SkipTestNG.skipIf(!useAutoCommit, "requires auto-commit to be true");

      int dataSize = 10;
      Cache<String, String> cacheOne = cache(0, cacheName);
      Cache<String, String> cacheTwo = cache(1, cacheName);

      PersistenceManager persistenceManager = TestingUtil.extractComponent(cache(0, cacheName), PersistenceManager.class);
      JdbcStringBasedStore jdbcStringBasedStore = persistenceManager.getStores(JdbcStringBasedStore.class).iterator().next();
      ConnectionFactory connectionFactory = jdbcStringBasedStore.getConnectionFactory();
      Connection connection = connectionFactory.getConnection();

      final TransactionManager transactionManager = cacheOne.getAdvancedCache().getTransactionManager();
      try {
         transactionManager.begin();
         for (int i = 0; i < dataSize; i++) {
            String key = getStringKeyForCache((i & 1) == 1 ? cacheOne : cacheTwo);
            cacheOne.put(key, "value-" + i);
         }
         transactionManager.rollback();
      } finally {
         connectionFactory.releaseConnection(connection);
      }

      assertTrue(cacheOne.isEmpty());
      assertTrue(cacheTwo.isEmpty());

      assertEquals(0, UnitTestDatabaseManager.rowCount(connectionFactory, jdbcStringBasedStore.getTableManager().getDataTableName()));
   }

   private void assertEntryInDatabase(Connection connection, String key, String tableName, boolean exists) throws SQLException {
      String sql = String.format("SELECT ID_COLUMN FROM %s WHERE ID_COLUMN = ?", tableName);
      try (PreparedStatement stat = connection.prepareStatement(sql)) {
         stat.setString(1, key);
         try (ResultSet rs = stat.executeQuery()) {
            assertEquals(exists, rs.next());
         }
      }
   }

   private void performCacheOperation(Cache<?, ?> cache, Runnable runnable) throws Throwable {
      if (!useAutoCommit) {
         final TransactionManager transactionManager = cache.getAdvancedCache().getTransactionManager();
         try {
            transactionManager.begin();
            runnable.run();
            transactionManager.commit();
         } catch (Exception e) {
            transactionManager.rollback();
            throw e;
         }
         return;
      }

      runnable.run();
   }
}
