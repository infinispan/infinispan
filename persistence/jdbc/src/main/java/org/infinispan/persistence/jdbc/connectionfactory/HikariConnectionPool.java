package org.infinispan.persistence.jdbc.connectionfactory;

import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.infinispan.commons.util.FileLookup;
import org.infinispan.commons.util.FileLookupFactory;
import org.infinispan.persistence.jdbc.configuration.PooledConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.logging.LogFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;

/**
 * @author Ryan Emerson
 */
public class HikariConnectionPool implements ConnectionPool {

   private static final Log log = LogFactory.getLog(HikariConnectionPool.class, Log.class);

   private static final String HIKARI_PROPERTIES = "hikari.properties";

   private HikariDataSource hikari;
   private HikariPoolMXBean hikariMxBean;

   public HikariConnectionPool(ClassLoader classLoader, PooledConnectionFactoryConfiguration poolConfig) {
      try {
         Properties properties = loadPropertiesFile(classLoader, poolConfig);
         if (poolConfig.connectionUrl() != null)
            properties.setProperty("jdbcUrl", poolConfig.connectionUrl());
         if (poolConfig.driverClass() != null)
            properties.setProperty("driverClassName", poolConfig.driverClass());
         if (poolConfig.username() != null)
            properties.setProperty("dataSource.user", poolConfig.username());
         if (poolConfig.password() != null)
            properties.setProperty("dataSource.password", poolConfig.password());

         HikariConfig hikariConfig = new HikariConfig(properties);
         hikariConfig.setRegisterMbeans(true);
         hikari = new HikariDataSource(hikariConfig);
      } catch (Exception e) {
         log.errorCreatingHikariCP(e);
         throw new PersistenceException("Error creating HikariCP instance: ", e);
      }
   }

   // Load Hikari properties in priority of properties file specified in configuration then hikari.properties on the classpath
   private Properties loadPropertiesFile(ClassLoader classLoader, PooledConnectionFactoryConfiguration poolConfig) {
      if (classLoader == null)
         return new Properties();

      FileLookup fileLookup = FileLookupFactory.newInstance();
      String propertyPath = poolConfig.propertyFile();
      InputStream is = null;
      try {
         if (propertyPath != null) {
            is = fileLookup.lookupFileStrict(propertyPath, classLoader);
         } else if (classLoader.getResource(HIKARI_PROPERTIES) != null) {
            is = fileLookup.lookupFileStrict(HIKARI_PROPERTIES, classLoader);
         }

         if (is != null) {
            Properties properties = new Properties();
            properties.load(is);
            return properties;
         }
      } catch (IOException e) {
         log.errorLoadingHikariCPProperties(PooledConnectionFactoryConfiguration.class.getName());
      }
      return new Properties();
   }

   @Override
   public void close() {
      if (hikari != null)
         hikari.close();
   }

   @Override
   public Connection getConnection() throws SQLException {
      return hikari.getConnection();
   }

   @Override
   public int getMaxPoolSize() {
      return hikari.getMaximumPoolSize();
   }

   @Override
   public int getNumConnectionsAllUsers() throws SQLException {
      // Only create hikari mbean if required
      if (hikariMxBean == null)
         initMBean();
      return hikariMxBean.getActiveConnections();
   }

   @Override
   public int getNumBusyConnectionsAllUsers() throws SQLException {
      if (hikariMxBean == null)
         initMBean();
      return hikariMxBean.getActiveConnections();
   }

   private void initMBean() {
      try {
         MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
         ObjectName poolName = new ObjectName("com.zaxxer.hikari:type=Pool (" + hikari.getPoolName() + ")");
         hikariMxBean = JMX.newMXBeanProxy(mBeanServer, poolName, HikariPoolMXBean.class);
      } catch (MalformedObjectNameException ignore) {
         // Ignore as this should never happen
      }
   }
}
