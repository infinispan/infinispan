/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.loaders.jdbc;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.infinispan.loaders.CacheLoaderException;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactoryConfig;
import org.infinispan.loaders.jdbc.connectionfactory.PooledConnectionFactory;
import org.infinispan.test.fwk.UnitTestDatabaseManager;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * Tester class for {@link org.infinispan.loaders.jdbc.TableManipulation}.
 *
 * @author Mircea.Markus@jboss.com
 */
@Test(groups = "functional", testName = "loaders.jdbc.TableManipulationTest")
public class TableManipulationTest {
   Connection connection;
   TableManipulation tableManipulation;
   private ConnectionFactoryConfig cfg;

   @BeforeTest
   public void createConnection() throws Exception {
      cfg = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
      connection = DriverManager.getConnection(cfg.getConnectionUrl(), cfg.getUserName(), cfg.getPassword());
      tableManipulation = UnitTestDatabaseManager.buildStringTableManipulation();
      tableManipulation.setCacheName("aName");
   }

   @AfterTest(alwaysRun = true)
   public void closeConnection() throws SQLException {
      connection.close();
   }

   public void testConnectionLeakGuessDatabaseType() throws Exception {
      TableManipulation tableManipulation = UnitTestDatabaseManager.buildStringTableManipulation();
      // database type must now be determined
      tableManipulation.databaseType = null;
      tableManipulation.setCacheName("GuessDatabaseType");

      PooledConnectionFactory factory = new PooledConnectionFactory();
      ConnectionFactoryConfig config = UnitTestDatabaseManager.getUniqueConnectionFactoryConfig();
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

      other.setDataColumnType(null);
      try {
         other.createTable(mockConnection);
         assert false : "missing config param, exception expected";
      } catch (CacheLoaderException e) {
         other.setDataColumnType("VARCHAR(255)");
      }

      other.createTable(mockConnection);

      other.setIdColumnName("");
      try {
         other.createTable(mockConnection);
         assert false : "missing config param, exception expected";
      } catch (CacheLoaderException e) {
         other.setIdColumnName("abc");
      }

      other.createTable(mockConnection);

      other.setDataColumnName(null);
      try {
         other.createTable(mockConnection);
         assert false : "missing config param, exception expected";
      } catch (CacheLoaderException e) {
         other.setDataColumnName("abc");
      }

      other.createTable(mockConnection);

      other.setDataColumnName(null);
      try {
         other.createTable(mockConnection);
         assert false : "missing config param, exception expected";
      } catch (CacheLoaderException e) {
         other.setDataColumnName("abc");
      }

      other.createTable(mockConnection);

      other.setTimestampColumnName(null);
      try {
         other.createTable(mockConnection);
         assert false : "missing config param, exception expected";
      } catch (CacheLoaderException e) {
         other.setDataColumnName("timestamp");
      }

      other.setTimestampColumnType(null);
      try {
         other.createTable(mockConnection);
         assert false : "missing config param, exception expected";
      } catch (CacheLoaderException e) {
         other.setIdColumnType("BIGINT");
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
