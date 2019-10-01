package org.infinispan.persistence.jdbc;

import static org.infinispan.persistence.jdbc.logging.Log.PERSISTENCE;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Contains common methods used by JDBC CacheStores.
 *
 * @author Mircea.Markus@jboss.com
 */
public class JdbcUtil {
   public static void safeClose(Statement ps) {
      if (ps != null) {
         try {
            ps.close();
         } catch (SQLException e) {
            PERSISTENCE.sqlFailureUnexpected(e);
         }
      }
   }

   public static void safeClose(Connection connection) {
      if (connection != null) {
         try {
            connection.close();
         } catch (SQLException e) {
            PERSISTENCE.sqlFailureClosingConnection(connection, e);
         }
      }
   }

   public static void safeClose(ResultSet rs) {
      if (rs != null) {
         try {
            rs.close();
         } catch (SQLException e) {
            PERSISTENCE.sqlFailureUnexpected(e);
         }
      }
   }
}
