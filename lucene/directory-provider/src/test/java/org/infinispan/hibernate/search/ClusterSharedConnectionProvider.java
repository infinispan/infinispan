package org.infinispan.hibernate.search;

import org.h2.jdbcx.JdbcConnectionPool;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 */
public class ClusterSharedConnectionProvider implements ConnectionProvider {

   public static volatile JdbcConnectionPool pool;

   public static void realStart() {
      pool = JdbcConnectionPool.create("jdbc:h2:mem:dbsearch", "sa", "sa");
   }

   public static void realStop() {
      if (pool != null) {
         pool.dispose();
         pool = null;
      }
   }

   @Override
   public Connection getConnection() throws SQLException {
      return pool.getConnection();
   }

   @Override
   public void closeConnection(Connection conn) throws SQLException {
      conn.close();
   }

   @Override
   public boolean supportsAggressiveRelease() {
      return false;
   }

   @Override
   public boolean isUnwrappableAs(Class unwrapType) {
      return false;
   }

   @Override
   public <T> T unwrap(Class<T> unwrapType) {
      return null;
   }
}
