package org.infinispan.persistence.jdbc.connectionfactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;
import org.infinispan.persistence.CacheLoaderException;
import org.infinispan.persistence.jdbc.JdbcUtil;
import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.PooledConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.util.logging.LogFactory;

import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Pooled connection factory based on C3P0. For a complete configuration reference, look <a
 * href="http://www.mchange.com/projects/c3p0/index.html#configuration">here</a>. The connection pool can be configured
 * in various ways, as described <a href="http://www.mchange.com/projects/c3p0/index.html#configuration_files">here</a>.
 * The simplest way is by having an <tt>c3p0.properties</tt> file in the classpath. If no such file is found, default,
 * hardcoded values will be used.
 *
 * @author Mircea.Markus@jboss.com
 * @author Tristan Tarrant
 */
public class PooledConnectionFactory extends ConnectionFactory {

   private static final Log log = LogFactory.getLog(PooledConnectionFactory.class, Log.class);
   private ComboPooledDataSource pooledDataSource;

   @Override
   public void start(ConnectionFactoryConfiguration config, ClassLoader classLoader) throws CacheLoaderException {
      logFileOverride(classLoader);
      PooledConnectionFactoryConfiguration pooledConfiguration;
      if (config instanceof PooledConnectionFactoryConfiguration) {
         pooledConfiguration = (PooledConnectionFactoryConfiguration) config;
      }
      else {
         throw new CacheLoaderException("ConnectionFactoryConfiguration passed in must be an instance of " +
               "PooledConnectionFactoryConfiguration");
      }
      pooledDataSource = new ComboPooledDataSource();
      pooledDataSource.setProperties(new Properties());
      try {
         /* Since c3p0 does not throw an exception when it fails to load a driver we attempt to do so here
          * Also, c3p0 does not allow specifying a custom classloader, so use c3p0's
          */
         Class.forName(pooledConfiguration.driverClass(), true, ComboPooledDataSource.class.getClassLoader());
         pooledDataSource.setDriverClass(pooledConfiguration.driverClass()); //loads the jdbc driver
      } catch (Exception e) {
         log.errorInstantiatingJdbcDriver(pooledConfiguration.driverClass(), e);
         throw new CacheLoaderException(String.format(
               "Error while instatianting JDBC driver: '%s'", pooledConfiguration.driverClass()), e);
      }
      pooledDataSource.setJdbcUrl(pooledConfiguration.connectionUrl());
      pooledDataSource.setUser(pooledConfiguration.username());
      pooledDataSource.setPassword(pooledConfiguration.password());
      if (log.isTraceEnabled()) {
         log.tracef("Started connection factory with config: %s", config);
      }
   }

   private void logFileOverride(ClassLoader classLoader) {
      URL propsUrl = FileLookupFactory.newInstance().lookupFileLocation("c3p0.properties", classLoader);
      URL xmlUrl = FileLookupFactory.newInstance().lookupFileLocation("c3p0-config.xml", classLoader);
      if (log.isDebugEnabled() && propsUrl != null) {
         log.debugf("Found 'c3p0.properties' in classpath: %s", propsUrl);
      }
      if (log.isDebugEnabled() && xmlUrl != null) {
         log.debugf("Found 'c3p0-config.xml' in classpath: %s", xmlUrl);
      }
   }

   @Override
   public void stop() {
      try {
         DataSources.destroy(pooledDataSource);
         if (log.isDebugEnabled()) {
            log.debug("Successfully stopped PooledConnectionFactory.");
         }
      }
      catch (SQLException sqle) {
         log.couldNotDestroyC3p0ConnectionPool(pooledDataSource!=null?pooledDataSource.toString():null, sqle);
      }
   }

   @Override
   public Connection getConnection() throws CacheLoaderException {
      try {
         logBefore(true);
         Connection connection = pooledDataSource.getConnection();
         logAfter(connection, true);
         return connection;
      } catch (SQLException e) {
         throw new CacheLoaderException("Failed obtaining connection from PooledDataSource", e);
      }
   }

   @Override
   public void releaseConnection(Connection conn) {
      logBefore(false);
      JdbcUtil.safeClose(conn);
      logAfter(conn, false);
   }

   public ComboPooledDataSource getPooledDataSource() {
      return pooledDataSource;
   }

   private void logBefore(boolean checkout) {
      if (log.isTraceEnabled()) {
         String operation = checkout ? "checkout" : "release";
         try {
            log.tracef("DataSource before %s (NumBusyConnectionsAllUsers) : %d, (NumConnectionsAllUsers) : %d",
                       operation, pooledDataSource.getNumBusyConnectionsAllUsers(), pooledDataSource.getNumConnectionsAllUsers());
         } catch (SQLException e) {
            log.sqlFailureUnexpected(e);
         }
      }
   }

   private void logAfter(Connection connection, boolean checkout)  {
      if (log.isTraceEnabled()) {
         String operation = checkout ? "checkout" : "release";
         try {
            log.tracef("DataSource after %s (NumBusyConnectionsAllUsers) : %d, (NumConnectionsAllUsers) : %d",
                      operation, pooledDataSource.getNumBusyConnectionsAllUsers(), pooledDataSource.getNumConnectionsAllUsers());
         } catch (SQLException e) {
            log.sqlFailureUnexpected(e);
         }
         log.tracef("Connection %s : %s", operation, connection);
      }
   }
}
