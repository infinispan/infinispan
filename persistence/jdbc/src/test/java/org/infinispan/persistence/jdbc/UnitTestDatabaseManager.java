package org.infinispan.persistence.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.infinispan.persistence.jdbc.common.JdbcUtil;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.configuration.TableManipulationConfigurationBuilder;
import org.infinispan.persistence.jdbc.impl.table.TableName;

/**
 * Class that assures concurrent access to the in memory database.
 *
 * @author Mircea.Markus@jboss.com
 * @author Navin Surtani (<a href="mailto:nsurtani@redhat.com">nsurtani@redhat.com</a>)
 * @author Tristan Tarrant
 */

public class UnitTestDatabaseManager extends org.infinispan.persistence.jdbc.common.UnitTestDatabaseManager {
   public static void buildTableManipulation(TableManipulationConfigurationBuilder<?, ?> table) {
      table
            .tableNamePrefix("ISPN_STRING")
            .idColumnName("ID_COLUMN")
            .idColumnType("VARCHAR(255)")
            .dataColumnName("DATA_COLUMN")
            .dataColumnType("BLOB")
            .timestampColumnName("TIMESTAMP_COLUMN")
            .timestampColumnType("BIGINT")
            .segmentColumnName("SEGMENT_COLUMN")
         .segmentColumnType("INTEGER");
   }

   /**
    * Counts the number of rows in the given table.
    */
   public static int rowCount(ConnectionFactory connectionFactory, TableName tableName) {

      Connection conn = null;
      PreparedStatement statement = null;
      ResultSet resultSet = null;
      try {
         conn = connectionFactory.getConnection();
         String sql = "SELECT count(*) FROM " + tableName;
         statement = conn.prepareStatement(sql);
         resultSet = statement.executeQuery();
         resultSet.next();
         return resultSet.getInt(1);
      } catch (Exception ex) {
         throw new RuntimeException(ex);
      } finally {
         org.infinispan.persistence.jdbc.common.JdbcUtil.safeClose(resultSet);
         JdbcUtil.safeClose(statement);
         connectionFactory.releaseConnection(conn);
      }
   }
}
