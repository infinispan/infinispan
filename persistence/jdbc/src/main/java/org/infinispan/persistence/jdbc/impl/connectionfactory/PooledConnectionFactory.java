package org.infinispan.persistence.jdbc.impl.connectionfactory;

import java.sql.Connection;
import java.sql.SQLException;

import org.infinispan.persistence.jdbc.JdbcUtil;
import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.PooledConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.connectionfactory.C3P0ConnectionPool;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionPool;
import org.infinispan.persistence.jdbc.connectionfactory.HikariConnectionPool;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.logging.LogFactory;

/**
 * Pooled connection factory that uses HikariCP by default. In order to utilise the legacy connection pool, C3P0, users
 * must pass the system property <tt>infinispan.jdbc.c3p0.force</tt> with the value true.
 *
 * HikariCP property files can be specified by explicitly stating its path or name (if the file is on the classpath) via
 * PooledConnectionFactoryConfiguration.propertyFile field.  Or by ensuring that a <tt>hikari.properties</tt> file is
 * on the classpath. Note, that the file specified by <tt>propertyField</tt> takes precedence over <tt>hikari.properties</tt>.
 *
 * For a complete configuration reference for C3P0 look <a href="http://www.mchange.com/projects/c3p0/index.html#configuration">here</a>.
 * The connection pool can be configured n various ways, as described
 * <a href="http://www.mchange.com/projects/c3p0/index.html#configuration_files">here</a>. The simplest way is by having
 * an <tt>c3p0.properties</tt> file in the classpath.
 *
 * If no properties files are found for either HikariCP or C3PO then the default values of these connection pools are
 * utilised.
 *
 * @author Mircea.Markus@jboss.com
 * @author Tristan Tarrant
 * @author Ryan Emerson
 */
public class PooledConnectionFactory extends ConnectionFactory {

   private static final Log log = LogFactory.getLog(PooledConnectionFactory.class, Log.class);
   private static boolean trace = log.isTraceEnabled();

   private ConnectionPool connectionPool;

   @Override
   public void start(ConnectionFactoryConfiguration config, ClassLoader classLoader) throws PersistenceException {
      PooledConnectionFactoryConfiguration poolConfig;
      if (config instanceof PooledConnectionFactoryConfiguration) {
         poolConfig = (PooledConnectionFactoryConfiguration) config;
      } else {
         throw new PersistenceException("ConnectionFactoryConfiguration passed in must be an instance of " +
                                              "PooledConnectionFactoryConfiguration");
      }

      connectionPool = C3P0ConnectionPool.forceC3P0() ? new C3P0ConnectionPool(classLoader, poolConfig) :
            new HikariConnectionPool(classLoader, poolConfig);
      if (trace) log.tracef("Started connection factory with config: %s", config);
   }

   @Override
   public void stop() {
      if (connectionPool != null) {
         connectionPool.close();
         if (trace) log.debug("Successfully stopped PooledConnectionFactory.");
      }
   }

   @Override
   public Connection getConnection() throws PersistenceException {
      try {
         logBefore(true);
         Connection connection = connectionPool.getConnection();
         logAfter(connection, true);
         return connection;
      } catch (SQLException e) {
         throw new PersistenceException("Failed obtaining connection from PooledDataSource", e);
      }
   }

   @Override
   public void releaseConnection(Connection conn) {
      logBefore(false);
      JdbcUtil.safeClose(conn);
      logAfter(conn, false);
   }

   public int getMaxPoolSize() {
      return connectionPool.getMaxPoolSize();
   }

   public int getNumConnectionsAllUsers() throws SQLException {
      return connectionPool.getNumConnectionsAllUsers();
   }

   public int getNumBusyConnectionsAllUsers() throws SQLException {
      return connectionPool.getNumBusyConnectionsAllUsers();
   }

   private void logBefore(boolean checkout) {
      log(null, checkout, true);
   }

   private void logAfter(Connection connection, boolean checkout) {
      log(connection, checkout, false);
   }

   private void log(Connection connection, boolean checkout, boolean before)  {
      if (trace) {
         String stage = before ? "before" : "after";
         String operation = checkout ? "checkout" : "release";
         try {
            log.tracef("DataSource %s %s (NumBusyConnectionsAllUsers) : %d, (NumConnectionsAllUsers) : %d",
                       stage, operation, getNumBusyConnectionsAllUsers(), getNumConnectionsAllUsers());
         } catch (SQLException e) {
            log.sqlFailureUnexpected(e);
         }

         if (connection != null)
            log.tracef("Connection %s : %s", operation, connection);
      }
   }
}
