package org.infinispan.persistence.jdbc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.marshall.TestObjectStreamMarshaller;
import org.infinispan.persistence.DummyInitializationContext;
import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.PooledConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.SimpleConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.connectionfactory.PooledConnectionFactory;
import org.infinispan.persistence.jdbc.impl.connectionfactory.SimpleConnectionFactory;
import org.infinispan.persistence.jdbc.impl.table.TableManager;
import org.infinispan.persistence.jdbc.impl.table.TableManagerFactory;
import org.infinispan.persistence.jdbc.impl.table.TableName;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tester class for {@link TableManager}.
 *
 * @author Mircea.Markus@jboss.com
 * @author Ryan Emerson
 */
@Test(groups = "functional", testName = "persistence.jdbc.TableManagerTest")
public class TableManagerTest extends AbstractInfinispanTest {
   protected ConnectionFactory connectionFactory;
   protected Connection connection;
   protected TableManager tableManager;
   protected InitializationContext ctx;

   @BeforeClass
   public void createConnection() throws Exception {
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      UnitTestDatabaseManager.setDialect(storeBuilder);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table());
      ConnectionFactoryConfiguration factoryConfiguration = UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder).create();

      if (factoryConfiguration instanceof SimpleConnectionFactoryConfiguration) {
         SimpleConnectionFactoryConfiguration simpleConfiguration = (SimpleConnectionFactoryConfiguration)
               factoryConfiguration;
         connectionFactory = ConnectionFactory.getConnectionFactory(SimpleConnectionFactory.class);
         connectionFactory.start(simpleConfiguration, connectionFactory.getClass().getClassLoader());
         connection = connectionFactory.getConnection();

      } else if (factoryConfiguration instanceof PooledConnectionFactoryConfiguration) {
         PooledConnectionFactoryConfiguration pooledConfiguration = (PooledConnectionFactoryConfiguration)
               factoryConfiguration;

         connectionFactory = ConnectionFactory.getConnectionFactory(PooledConnectionFactory.class);
         connectionFactory.start(pooledConfiguration, connectionFactory.getClass().getClassLoader());
         connection = connectionFactory.getConnection();
      }
      Cache<?, ?> cache = mock(Cache.class);
      when(cache.getCacheConfiguration()).thenReturn(new ConfigurationBuilder().build());

      StoreConfiguration storeConfig = mock(StoreConfiguration.class);
      when(storeConfig.segmented()).thenReturn(false);

      ctx = new DummyInitializationContext(storeConfig, cache, new TestObjectStreamMarshaller(), null, null, null, null, null, null, null);
      tableManager = TableManagerFactory.getManager(ctx, connectionFactory, storeBuilder.create(), "aName");
   }

   @AfterClass(alwaysRun = true)
   public void closeConnection() throws SQLException {
      connection.close();
      connectionFactory.stop();
   }

   public void testConnectionLeakGuessDialect() {
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
//      storeBuilder.table().createOnStart(false);

      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table());
      PooledConnectionFactory connectionFactory = new PooledConnectionFactory();
      ConnectionFactoryConfiguration config = UnitTestDatabaseManager
            .configureUniqueConnectionFactory(storeBuilder).create();
      connectionFactory.start(config, Thread.currentThread().getContextClassLoader());

      // JdbcStringBasedStoreConfiguration defaults to null dialect, so dialect and versions must be guessed
      TableManager tableManager = TableManagerFactory.getManager(ctx, connectionFactory, storeBuilder.create(), "GuessDialect");
      tableManager.start();
      UnitTestDatabaseManager.verifyConnectionLeaks(connectionFactory);
      tableManager.stop();
      connectionFactory.stop();
   }

   public void testCreateTable() throws Exception {
      assert !existsTable(connection, tableManager.getDataTableName());
      assert !existsTable(connection, tableManager.getMetaTableName());
      tableManager.createDataTable(connection);
      tableManager.createMetaTable(connection);
      assert existsTable(connection, tableManager.getDataTableName());
      assert existsTable(connection, tableManager.getMetaTableName());
   }

   @Test(dependsOnMethods = "testCreateTable")
   public void testExists() throws PersistenceException {
      assert tableManager.tableExists(connection, tableManager.getDataTableName());
      assert tableManager.tableExists(connection, tableManager.getMetaTableName());
      assert !tableManager.tableExists(connection, new TableName("\"", "", "does_not_exist"));
   }

   @Test(dependsOnMethods = "testExists")
   public void testDrop() throws Exception {
      TableName dataTableName = tableManager.getDataTableName();
      TableName metaTableName = tableManager.getMetaTableName();
      assert tableManager.tableExists(connection, dataTableName);
      assert tableManager.tableExists(connection, metaTableName);
      byte[] data = new byte[64];
      new Random().nextBytes(data);
      PreparedStatement ps = null;
      try {
         ps = connection.prepareStatement("INSERT INTO " + tableManager.getDataTableName() + "(ID_COLUMN, DATA_COLUMN, TIMESTAMP_COLUMN, SEGMENT_COLUMN) values(?, ?, ?, ?)");
         ps.setString(1, System.currentTimeMillis() + "");
         ps.setBytes(2, data);
         ps.setLong(3, System.currentTimeMillis());
         ps.setLong(4, 1);
         assert 1 == ps.executeUpdate();
      } finally {
         JdbcUtil.safeClose(ps);
      }
      tableManager.dropTables(connection);
      assert !tableManager.tableExists(connection, dataTableName);
      assert !tableManager.tableExists(connection, metaTableName);
   }

   public void testTableQuoting() throws Exception {
      TableName dataTableName = tableManager.getDataTableName();
      tableManager.dropDataTable(connection);
      assert !existsTable(connection, dataTableName);
      tableManager.createDataTable(connection);
      assert existsTable(connection, tableManager.getDataTableName());
   }

   static boolean existsTable(Connection connection, TableName tableName) throws Exception {
      Statement st = connection.createStatement();
      ResultSet rs = null;
      try {
         rs = st.executeQuery("select * from " + tableName);
         return true;
      } catch (SQLException e) {
         return false;
      } finally {
         JdbcUtil.safeClose(rs);
         JdbcUtil.safeClose(st);
      }
   }
}
