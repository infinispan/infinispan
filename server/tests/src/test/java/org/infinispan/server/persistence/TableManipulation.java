package org.infinispan.server.persistence;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Enumeration;

import org.infinispan.commons.marshall.ProtoStreamMarshaller;
import org.infinispan.commons.marshall.WrappedByteArray;
import org.infinispan.persistence.jdbc.common.DatabaseType;
import org.infinispan.persistence.jdbc.common.JdbcUtil;
import org.infinispan.persistence.jdbc.common.configuration.PooledConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.common.configuration.PooledConnectionFactoryConfigurationBuilder;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.impl.connectionfactory.PooledConnectionFactory;
import org.infinispan.persistence.keymappers.DefaultTwoWayKey2StringMapper;

import com.mysql.cj.jdbc.AbandonedConnectionCleanupThread;

public class TableManipulation implements AutoCloseable {

   private ConnectionFactory connectionFactory;
   private Connection connection;
   public PooledConnectionFactoryConfiguration poolConfiguration;
   private final String countRowsSql;
   private final String selectIdRowSqlWithLike;
   private static final String ID_COLUMN_NAME = "id";
   private static final String DEFAULT_IDENTIFIER_QUOTE_STRING = "\"";
   private static final String TABLE_NAME_PREFIX = "tbl";
   private String tableName;

   public TableManipulation(String cacheName, PooledConnectionFactoryConfigurationBuilder builder) {
      this.poolConfiguration = builder.create();
      this.tableName = String.format("%s%s_%s%s", DEFAULT_IDENTIFIER_QUOTE_STRING, TABLE_NAME_PREFIX, cacheName, DEFAULT_IDENTIFIER_QUOTE_STRING);
      if (isMysql()) {
         this.tableName = this.tableName.replaceAll("\"", "");
      }
      this.countRowsSql = "SELECT COUNT(*) FROM " + tableName;
      this.selectIdRowSqlWithLike = String.format("SELECT %s FROM %s WHERE %s LIKE ?", ID_COLUMN_NAME, tableName, ID_COLUMN_NAME);
   }

   private ConnectionFactory getConnectionFactory() {
      connectionFactory = ConnectionFactory.getConnectionFactory(PooledConnectionFactory.class);
      connectionFactory.start(poolConfiguration, connectionFactory.getClass().getClassLoader());
      return connectionFactory;
   }

   private Connection getConnection() {
      if (connection == null) {
         connection = getConnectionFactory().getConnection();
      }
      return connection;
   }

   public String getValueByKey(String key) throws Exception {
      Connection connection = getConnection();
      PreparedStatement ps = null;
      ResultSet rs = null;
      try {
         ps = connection.prepareStatement(selectIdRowSqlWithLike);
         ps.setString(1, "%" + getEncodedKey(key) + "%");
         rs = ps.executeQuery();
         if (rs.next()) {
            return rs.getString("ID");
         }
         return null;
      } finally {
         JdbcUtil.safeClose(ps);
         JdbcUtil.safeClose(rs);
      }
   }

   public int countAllRows() {
      connection = getConnection();
      try (PreparedStatement ps = connection.prepareStatement(countRowsSql);
           ResultSet rs = ps.executeQuery()) {
         if (rs.next()) {
            return rs.getInt(1);
         } else {
            throw new IllegalStateException(countRowsSql + " returned no rows");
         }
      } catch (SQLException e) {
         throw new RuntimeException(e);
      }
   }

   public String getEncodedKey(String key) throws Exception {
      ProtoStreamMarshaller protoStreamMarshaller = new ProtoStreamMarshaller();
      byte[] marshalled = protoStreamMarshaller.objectToByteBuffer(key);
      DefaultTwoWayKey2StringMapper mapper = new DefaultTwoWayKey2StringMapper();
      return mapper.getStringMapping(new WrappedByteArray(marshalled));
   }

   private void deregisterDrivers() {
      Enumeration<Driver> drivers = DriverManager.getDrivers();
      Driver driver;
      while (drivers.hasMoreElements()) {
         try {
            driver = drivers.nextElement();
            if (driver instanceof com.mysql.cj.jdbc.Driver) {
               AbandonedConnectionCleanupThread.checkedShutdown();
            }
            DriverManager.deregisterDriver(driver);
         } catch (SQLException ex) {
            ex.printStackTrace();
         }
      }
   }

   private boolean isMysql() {
      try {
         connection = getConnection();
         String dbProduct = connection.getMetaData().getDatabaseProductName();
         DatabaseType databaseType = DatabaseType.guessDialect(dbProduct);
         if (databaseType == DatabaseType.MYSQL) {
            return true;
         }
      } catch (SQLException e) {
         throw new RuntimeException(e);
      }
      return false;
   }

   @Override
   public void close() {
      if (connection != null) {
         JdbcUtil.safeClose(connection);
         connectionFactory.stop();
         connectionFactory.releaseConnection(connection);
      }
      deregisterDrivers();
   }
}
