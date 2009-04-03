package org.horizon.loader.jdbc.connectionfactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.horizon.loader.CacheLoaderException;
import org.horizon.loader.jdbc.JdbcUtil;

import java.beans.PropertyVetoException;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Pooled connection factory based on C3P0. For a complete configuration reference, look <a
 * href="http://www.mchange.com/projects/c3p0/index.html#configuration">here</a>. The connection pool can be configured
 * in various ways, as described <a href="http://www.mchange.com/projects/c3p0/index.html#configuration_files">here</a>.
 * The simplest way is by having an <tt>c3p0.properties</tt> file in the classpath. If no such file is found, default,
 * hardcoded valus will be used.
 *
 * @author Mircea.Markus@jboss.com
 */
public class PooledConnectionFactory extends ConnectionFactory {

   private static Log log = LogFactory.getLog(PooledConnectionFactory.class);
   private ComboPooledDataSource pooledDataSource;

   @Override
   public void start(ConnectionFactoryConfig config) throws CacheLoaderException {
      logFileOverride();
      pooledDataSource = new ComboPooledDataSource();
      pooledDataSource.setProperties(new Properties());
      try {
         pooledDataSource.setDriverClass(config.getDriverClass()); //loads the jdbc driver
      } catch (PropertyVetoException e) {
         String message = "Error while instatianting JDBC driver: '" + config.getDriverClass();
         log.error(message, e);
         throw new CacheLoaderException(message, e);
      }
      pooledDataSource.setJdbcUrl(config.getConnectionUrl());
      pooledDataSource.setUser(config.getUserName());
      pooledDataSource.setPassword(config.getPassword());
   }

   private void logFileOverride() {
      URL propsUrl = Thread.currentThread().getContextClassLoader().getResource("c3p0.properties");
      URL xmlUrl = Thread.currentThread().getContextClassLoader().getResource("c3p0-config.xml");
      if (log.isInfoEnabled() && propsUrl != null) {
         log.info("Found 'c3p0.properties' in classpath: " + propsUrl);
      }
      if (log.isInfoEnabled() && xmlUrl != null) {
         log.info("Found 'c3p0-config.xml' in classpath: " + xmlUrl);
      }
   }

   @Override
   public void stop() {
      try {
         DataSources.destroy(pooledDataSource);
         if (log.isTraceEnabled()) {
            log.debug("Sucessfully stopped PooledConnectionFactory.");
         }
      }
      catch (SQLException sqle) {
         log.warn("Could not destroy C3P0 connection pool: " + pooledDataSource, sqle);
      }
   }

   @Override
   public Connection getConnection() throws CacheLoaderException {
      try {
         if (log.isTraceEnabled()) {
            log.trace("DataSource before checkout (NumBusyConnectionsAllUsers) : " + pooledDataSource.getNumBusyConnectionsAllUsers());
            log.trace("DataSource before checkout (NumConnectionsAllUsers) : " + pooledDataSource.getNumConnectionsAllUsers());
         }
         Connection connection = pooledDataSource.getConnection();
         if (log.isTraceEnabled()) {
            log.trace("DataSource after checkout (NumBusyConnectionsAllUsers) : " + pooledDataSource.getNumBusyConnectionsAllUsers());
            log.trace("DataSource after checkout (NumConnectionsAllUsers) : " + pooledDataSource.getNumConnectionsAllUsers());
            log.trace("Connection checked out: " + connection);
         }
         return connection;
      } catch (SQLException e) {
         throw new CacheLoaderException("Failed obtaining connection from PooledDataSource", e);
      }
   }

   @Override
   public void releaseConnection(Connection conn) {
      JdbcUtil.safeClose(conn);
   }

   public ComboPooledDataSource getPooledDataSource() {
      return pooledDataSource;
   }
}
