package org.infinispan.loader.jdbc;

import static org.easymock.EasyMock.*;
import org.infinispan.loader.CacheLoaderException;
import org.infinispan.loader.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Tester class for {@link org.infinispan.loader.jdbc.TableManipulation}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "loader.jdbc.TableManipulationTest")
public class TableManipulationTest {
   Connection connection;
   TableManipulation tableManipulation;
   private ConnectionFactoryConfig cfg;

   @BeforeTest
   public void createConnection() throws Exception {
      cfg = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
      connection = DriverManager.getConnection(cfg.getConnectionUrl(), cfg.getUserName(), cfg.getPassword());
      tableManipulation = UnitTestDatabaseManager.buildDefaultTableManipulation();
   }

   @AfterTest
   public void closeConnection() throws SQLException {
      connection.close();
      UnitTestDatabaseManager.shutdownInMemoryDatabase(cfg);
   }

   public void testInsufficientConfigParams() throws Exception {
      Connection mockConnection = createNiceMock(Connection.class);
      Statement mockStatement = createNiceMock(Statement.class);
      expect(mockConnection.createStatement()).andReturn(mockStatement);
      expectLastCall().anyTimes();
      replay(mockConnection, mockStatement);
      TableManipulation other = tableManipulation.clone();
      try {
         other.createTable(mockConnection);
      } catch (CacheLoaderException e) {
         assert false : "We do not expect a failure here";
      }

      other.setDataColumnType(null);
      try {
         other.createTable(mockConnection);
         assert false : "missing config param, exception expected";
      } catch (CacheLoaderException e) {
         other.setDataColumnType("VARCHAR(255)");
         assert true : "We do not expect a failure here";
      }

      other.createTable(mockConnection);

      other.setIdColumnName("");
      try {
         other.createTable(mockConnection);
         assert false : "missing config param, exception expected";
      } catch (CacheLoaderException e) {
         other.setIdColumnName("abc");
         assert true : "We do not expect a failure here";
      }

      other.createTable(mockConnection);

      other.setDataColumnName(null);
      try {
         other.createTable(mockConnection);
         assert false : "missing config param, exception expected";
      } catch (CacheLoaderException e) {
         other.setDataColumnName("abc");
         assert true : "We do not expect a failure here";
      }

      other.createTable(mockConnection);

      other.setDataColumnName(null);
      try {
         other.createTable(mockConnection);
         assert false : "missing config param, exception expected";
      } catch (CacheLoaderException e) {
         other.setDataColumnName("abc");
         assert true : "We do not expect a failure here";
      }

      other.createTable(mockConnection);

      other.setTimestampColumnName(null);
      try {
         other.createTable(mockConnection);
         assert false : "missing config param, exception expected";
      } catch (CacheLoaderException e) {
         other.setDataColumnName("timestamp");
         assert true : "We do not expect a failure here";
      }

      other.setTimestampColumnType(null);
      try {
         other.createTable(mockConnection);
         assert false : "missing config param, exception expected";
      } catch (CacheLoaderException e) {
         other.setIdColumnType("BIGINT");
         assert true : "We do not expect a failure here";
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
      assert !tableManipulation.tableExists(connection, "does_not_exist");
   }

   @Test(dependsOnMethods = "testExists")
   public void testDrop() throws Exception {
      assert tableManipulation.tableExists(connection);
      PreparedStatement ps = null;
      try {
         ps = connection.prepareStatement("INSERT INTO horizon_jdbc(ID_COLUMN) values(?)");
         ps.setString(1, System.currentTimeMillis() + "");
         assert 1 == ps.executeUpdate();
      } finally {
         JdbcUtil.safeClose(ps);
      }
      tableManipulation.dropTable(connection);
      assert !tableManipulation.tableExists(connection);
   }

   static boolean existsTable(Connection connection, String tableName) throws Exception {
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
