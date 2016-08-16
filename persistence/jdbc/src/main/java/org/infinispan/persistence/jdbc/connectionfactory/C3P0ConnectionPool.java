package org.infinispan.persistence.jdbc.connectionfactory;

import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.persistence.jdbc.configuration.PooledConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.logging.LogFactory;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.DataSources;

/**
 * @deprecated Support for the C3P0 connection pool will be removed in 10.0
 * @author Ryan Emerson
 */
@Deprecated
public class C3P0ConnectionPool implements ConnectionPool {

   private static final Log log = LogFactory.getLog(C3P0ConnectionPool.class, Log.class);

   private static final String FORCE_C3P0 = "infinispan.jdbc.c3p0.force";
   private static final String C3P0_PROPERTIES = "c3p0.properties";
   private static final String C3P0_CONFIG = "c3p0-config.xml";

   private final ComboPooledDataSource c3p0 = new ComboPooledDataSource();

   public C3P0ConnectionPool(ClassLoader classLoader, PooledConnectionFactoryConfiguration poolConfig) {
      logWarnMessages(poolConfig);
      logFileOverride(classLoader);
      c3p0.setProperties(new Properties());
      try {
         /* Since c3p0 does not throw an exception when it fails to load a driver we attempt to do so here
          * Also, c3p0 does not allow specifying a custom classloader, so use c3p0's
          */
         Class.forName(poolConfig.driverClass(), true, ComboPooledDataSource.class.getClassLoader());
         c3p0.setDriverClass(poolConfig.driverClass()); //loads the jdbc driver
      } catch (Exception e) {
         log.errorInstantiatingJdbcDriver(poolConfig.driverClass(), e);
         throw new PersistenceException(String.format(
               "Error while instatianting JDBC driver: '%s'", poolConfig.driverClass()), e);
      }
      c3p0.setJdbcUrl(poolConfig.connectionUrl());
      c3p0.setUser(poolConfig.username());
      c3p0.setPassword(poolConfig.password());
   }

   public static boolean forceC3P0() {
      return Boolean.parseBoolean(System.getProperty(FORCE_C3P0));
   }

   private void logWarnMessages(PooledConnectionFactoryConfiguration poolConfig) {
      log.warn("The c3p0 connection factory has been deprecated and will be removed in future releases");

      if (poolConfig.propertyFile() != null) {
         log.warn(String.format("Ignoring properties file '%s'. In order to configure additional c3p0 properies, " +
                                      "you must place a '%s' or '%s' file on the classpath.",
                                poolConfig.propertyFile(), C3P0_CONFIG, C3P0_PROPERTIES));
      }
   }

   private void logFileOverride(ClassLoader classLoader) {
      FileLookup fileLookup = FileLookupFactory.newInstance();
      URL c3p0Props = fileLookup.lookupFileLocation(C3P0_PROPERTIES, classLoader);
      URL c3p0Xml = fileLookup.lookupFileLocation(C3P0_CONFIG, classLoader);
      if (log.isDebugEnabled()) {
         if (c3p0Props != null)
            log.debugf("Found '%s' in classpath: %s", C3P0_PROPERTIES, c3p0Props);
         if (c3p0Xml != null)
            log.debugf("Found '%s' in classpath: %s", C3P0_CONFIG, c3p0Xml);
      }
   }

   @Override
   public void close() {
      try {
         DataSources.destroy(c3p0);
      } catch (SQLException e) {
         log.couldNotDestroyC3p0ConnectionPool(c3p0 != null ? c3p0.toString() : null, e);
      }
   }

   @Override
   public Connection getConnection() throws SQLException {
      return c3p0.getConnection();
   }

   @Override
   public int getMaxPoolSize() {
      return c3p0.getMaxPoolSize();
   }

   @Override
   public int getNumConnectionsAllUsers() throws SQLException {
      return c3p0.getNumConnectionsAllUsers();
   }

   @Override
   public int getNumBusyConnectionsAllUsers() throws SQLException {
      return c3p0.getNumBusyConnectionsAllUsers();
   }
}
