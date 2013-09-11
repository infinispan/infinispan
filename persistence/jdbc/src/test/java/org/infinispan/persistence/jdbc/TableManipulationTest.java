package org.infinispan.persistence.jdbc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.JdbcStringBasedStoreConfigurationBuilder;
import org.infinispan.persistence.jdbc.configuration.PooledConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.SimpleConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.PooledConnectionFactory;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Tester class for {@link TableManipulation}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "persistence.jdbc.TableManipulationTest")
public class TableManipulationTest {
   Connection connection;
   TableManipulation tableManipulation;
   private ConnectionFactoryConfiguration factoryConfiguration;

   @BeforeTest
   public void createConnection() throws Exception {
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
               .addStore(JdbcStringBasedStoreConfigurationBuilder.class);
      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table(), false);
      factoryConfiguration = UnitTestDatabaseManager.configureUniqueConnectionFactory(storeBuilder).create();
      tableManipulation = new TableManipulation(storeBuilder.table().create());

      if (factoryConfiguration instanceof SimpleConnectionFactoryConfiguration) {
         SimpleConnectionFactoryConfiguration simpleConfiguration = (SimpleConnectionFactoryConfiguration)
               factoryConfiguration;
         connection = DriverManager.getConnection(simpleConfiguration.connectionUrl(),
               simpleConfiguration.username(), simpleConfiguration.password());

      } else if (factoryConfiguration instanceof PooledConnectionFactoryConfiguration) {
         PooledConnectionFactoryConfiguration pooledConfiguration = (PooledConnectionFactoryConfiguration)
               factoryConfiguration;
         connection = DriverManager.getConnection(pooledConfiguration.connectionUrl(),
               pooledConfiguration.username(), pooledConfiguration.password());
      }
      tableManipulation.setCacheName("aName");
   }

   @AfterTest
   public void closeConnection() throws SQLException {
      connection.close();
   }

   public void testConnectionLeakGuessDatabaseType() throws Exception {
      JdbcStringBasedStoreConfigurationBuilder storeBuilder = TestCacheManagerFactory
            .getDefaultCacheConfiguration(false)
            .persistence()
               .addStore(JdbcStringBasedStoreConfigurationBuilder.class);

      UnitTestDatabaseManager.buildTableManipulation(storeBuilder.table(), false);

      TableManipulation tableManipulation = new TableManipulation(storeBuilder.table().create());
      // database type must now be determined
      tableManipulation.databaseType = null;
      tableManipulation.setCacheName("GuessDatabaseType");

      PooledConnectionFactory factory = new PooledConnectionFactory();
      ConnectionFactoryConfiguration config = UnitTestDatabaseManager
            .configureUniqueConnectionFactory(storeBuilder).create();
      factory.start(config, Thread.currentThread().getContextClassLoader());
      tableManipulation.start(factory);
      tableManipulation.getUpdateRowSql();
      UnitTestDatabaseManager.verifyConnectionLeaks(factory);
      tableManipulation.stop();
      factory.stop();
   }

   public void testInsufficientConfigParams() throws Exception {
      Connection mockConnection = mock(Connection.class);
      Statement mockStatement = mock(Statement.class);
      when(mockConnection.createStatement()).thenReturn(mockStatement);
      TableManipulation other = tableManipulation.clone();
      try {
         other.createTable(mockConnection);
      } catch (CacheLoaderException e) {
         assert false : "We do not expect a failure here";
      }
   }

   public void testCreateTable() throws Exception {
      assert !existsTable(connection, tableManipulation.getTableName());
      tableManipulation.createTable(connection);
      assert existsTable(connection, tableManipulation.getTableName());
   }

   @Test(dependsOnMethods = "testCreateTable")
   public void testExists() throws CacheLoaderException {
      assert tableManipulation.tableExists(connection);
      assert !tableManipulation.tableExists(connection, new TableName("\"", "", "does_not_exist"));
   }

   public void testExistsWithSchema() throws CacheLoaderException {
     // todo
   }

   @Test(dependsOnMethods = "testExists")
   public void testDrop() throws Exception {
      assert tableManipulation.tableExists(connection);
      PreparedStatement ps = null;
      try {
         ps = connection.prepareStatement("INSERT INTO " + tableManipulation.getTableName() + "(ID_COLUMN) values(?)");
         ps.setString(1, System.currentTimeMillis() + "");
         assert 1 == ps.executeUpdate();
      } finally {
         JdbcUtil.safeClose(ps);
      }
      tableManipulation.dropTable(connection);
      assert !tableManipulation.tableExists(connection);
   }

   public void testTableQuoting() throws Exception {
      tableManipulation.setCacheName("my.cache");
      assert !existsTable(connection, tableManipulation.getTableName());
      tableManipulation.createTable(connection);
      assert existsTable(connection, tableManipulation.getTableName());
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
