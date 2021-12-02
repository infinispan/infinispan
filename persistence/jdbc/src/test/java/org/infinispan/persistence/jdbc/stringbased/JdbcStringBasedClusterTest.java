package org.infinispan.persistence.jdbc.stringbased;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfigurationBuilder;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.distribution.BaseDistStoreTest;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.jdbc.UnitTestDatabaseManager;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "persistence.jdbc.JdbcStringBasedClusterTest")
public class JdbcStringBasedClusterTest extends BaseDistStoreTest<Integer, String, JdbcStringBasedClusterTest> {

   private ControlledTimeService controlledTimeService = new ControlledTimeService();

   {
      INIT_CLUSTER_SIZE = 2;
      l1CacheEnabled = false;
   }

   @Override
   protected void amendCacheManagerBeforeStart(EmbeddedCacheManager ecm) {
      TestingUtil.replaceComponent(ecm, TimeService.class, controlledTimeService, true);
   }

   @Override
   public Object[] factory() {
      return new Object[]{
            new JdbcStringBasedClusterTest().segmented(true).shared(true).cacheMode(CacheMode.DIST_SYNC),
            new JdbcStringBasedClusterTest().segmented(false).shared(true).cacheMode(CacheMode.DIST_SYNC),
            new JdbcStringBasedClusterTest().segmented(true).cacheMode(CacheMode.LOCAL),
            new JdbcStringBasedClusterTest().segmented(false).cacheMode(CacheMode.LOCAL),
      };
   }

   @Override
   protected StoreConfigurationBuilder addStore(PersistenceConfigurationBuilder persistenceConfigurationBuilder, boolean shared) {
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = persistenceConfigurationBuilder
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      storeBuilder.shared(shared);
      storeBuilder.segmented(segmented);
      UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table());

      return storeBuilder;
   }

   public void testPurgeExpired() throws SQLException {
      Cache<String, String> cacheToUse = cache(0, cacheName);
      cacheToUse.put("key1", "expired", 10, TimeUnit.MINUTES);
      cacheToUse.put("key2", "value");
      cacheToUse.put("key3", "expired", 10, TimeUnit.MINUTES);
      cacheToUse.put("key4", "value");

      assertEquals(4, cache(0, cacheName).size());

      // Remove all entries from memory
      for (int i = 0; i < cacheManagers.size(); ++i) {
         TestingUtil.extractComponent(cache(i, cacheName), InternalDataContainer.class).clear();
      }

      assertEquals(4, cacheToUse.size());

      // Advance to expire one of the entries
      controlledTimeService.advance(TimeUnit.MINUTES.toMillis(11));

      PersistenceManager persistenceManager = TestingUtil.extractComponent(cache(0, cacheName), PersistenceManager.class);
      CompletionStages.join(persistenceManager.purgeExpired());

      assertEquals(2, cacheToUse.size());

      // Make sure the purge actually removed the data from the database
      JdbcStringBasedStore jdbcStringBasedStore = persistenceManager.getStores(JdbcStringBasedStore.class).iterator().next();
      ConnectionFactory connectionFactory = jdbcStringBasedStore.getConnectionFactory();
      Connection connection = connectionFactory.getConnection();
      try (Statement stat = connection.createStatement()) {
         try (ResultSet rs = stat.executeQuery("SELECT COUNT(1) FROM " + jdbcStringBasedStore.getTableManager().getDataTableName())) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
            assertFalse(rs.next());
         }
      } finally {
         connectionFactory.releaseConnection(connection);
      }
   }
}
