package org.infinispan.persistence.jdbc;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

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
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
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
   ConnectionFactory connectionFactory;
   Connection connection;
   TableManager tableManager;

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
      tableManager = TableManagerFactory.getManager(connectionFactory, storeBuilder.create(), "aName");
   }

   @AfterClass(alwaysRun = true)
   public void closeConnection() throws SQLException {
      connection.close();
      connectionFactory.stop();
   }

   public void testConnectionLeakGuessDialect() throws Exception {
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
            .addStore(JdbcStringBasedStoreConfigurationBuilder.class);

      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table());
      PooledConnectionFactory connectionFactory = new PooledConnectionFactory();
      ConnectionFactoryConfiguration config = UnitTestDatabaseManager
            .configureUniqueConnectionFactory(storeBuilder).create();
      connectionFactory.start(config, Thread.currentThread().getContextClassLoader());

      // JdbcStringBasedStoreConfiguration defaults to null dialect, so dialect and versions must be guessed
      TableManager tableManager = TableManagerFactory.getManager(connectionFactory, storeBuilder.create(), "GuessDialect");
      tableManager.start();
      UnitTestDatabaseManager.verifyConnectionLeaks(connectionFactory);
      tableManager.stop();
      connectionFactory.stop();
   }

   public void testCreateTable() throws Exception {
      assert !existsTable(connection, tableManager.getTableName());
      tableManager.createTable(connection);
      assert existsTable(connection, tableManager.getTableName());
   }

   @Test(dependsOnMethods = "testCreateTable")
   public void testExists() throws PersistenceException {
      assert tableManager.tableExists(connection);
      assert !tableManager.tableExists(connection, new TableName("\"", "", "does_not_exist"));
   }

   public void testExistsWithSchema() throws PersistenceException {
      // todo
   }

   @Test(dependsOnMethods = "testExists")
   public void testDrop() throws Exception {
      assert tableManager.tableExists(connection);
      byte[] data = new byte[64];
      new Random().nextBytes(data);
      PreparedStatement ps = null;
      try {
         ps = connection.prepareStatement("INSERT INTO " + tableManager.getTableName() + "(ID_COLUMN, DATA_COLUMN, TIMESTAMP_COLUMN) values(?, ?, ?)");
         ps.setString(1, System.currentTimeMillis() + "");
         ps.setBlob(2, new ByteArrayInputStream(data));
         ps.setLong(3, System.currentTimeMillis());
         assert 1 == ps.executeUpdate();
      } finally {
         JdbcUtil.safeClose(ps);
      }
      tableManager.dropTable(connection);
      assert !tableManager.tableExists(connection);
   }

   public void testTableQuoting() throws Exception {
      tableManager.dropTable(connection);
      assert !existsTable(connection, tableManager.getTableName());
      tableManager.createTable(connection);
      assert existsTable(connection, tableManager.getTableName());
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
