package org.infinispan.persistence.jdbc.connectionfactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;

import org.infinispan.persistence.jdbc.JdbcUtil;
import org.infinispan.persistence.jdbc.configuration.ConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.configuration.PooledConnectionFactoryConfiguration;
import org.infinispan.persistence.jdbc.logging.Log;
import org.infinispan.persistence.spi.PersistenceException;
import org.infinispan.util.logging.LogFactory;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalConnectionFactoryConfiguration;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalPropertiesReader;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;

/**
 * Pooled connection factory based upon Agroa https://agroal.github.io.
 *
 * @author Mircea.Markus@jboss.com
 * @author Tristan Tarrant
 * @author Ryan Emerson
 */
public class PooledConnectionFactory extends ConnectionFactory {

   private static final String PROPERTIES_PREFIX = "org.infinispan.agroal.";
   private static final Log log = LogFactory.getLog(PooledConnectionFactory.class, Log.class);
   private static boolean trace = log.isTraceEnabled();

   private AgroalDataSource dataSource;

   @Override
   public void start(ConnectionFactoryConfiguration config, ClassLoader classLoader) throws PersistenceException {
      PooledConnectionFactoryConfiguration poolConfig;
      if (config instanceof PooledConnectionFactoryConfiguration) {
         poolConfig = (PooledConnectionFactoryConfiguration) config;
      } else {
         throw new PersistenceException("ConnectionFactoryConfiguration passed in must be an instance of " +
                                              "PooledConnectionFactoryConfiguration");
      }

      try {

         String propsFile = poolConfig.propertyFile();
         if (propsFile != null) {
            dataSource = AgroalDataSource.from(new AgroalPropertiesReader(PROPERTIES_PREFIX).readProperties(propsFile));
         } else {
            // Default Agroal configuration with metrics disabled
            String password = poolConfig.password() != null ? poolConfig.password() : "";
            AgroalDataSourceConfigurationSupplier configuration = new AgroalDataSourceConfigurationSupplier()
                  .connectionPoolConfiguration(cp -> cp
                        .maxSize(10)
                        .acquisitionTimeout(Duration.ofSeconds(30))
                        .connectionFactoryConfiguration(cf -> cf
                              .jdbcUrl(poolConfig.connectionUrl())
                              .connectionProviderClassName(poolConfig.driverClass())
                              .jdbcTransactionIsolation(AgroalConnectionFactoryConfiguration.TransactionIsolation.UNDEFINED)
                              .principal(new NamePrincipal(poolConfig.username()))
                              .credential(new SimplePassword(password))
                        ));

            dataSource = AgroalDataSource.from(configuration);
         }
      } catch (Exception e) {
         throw new PersistenceException("Failed to create a AgroalDataSource", e);
      }
   }

   @Override
   public void stop() {
      if (dataSource != null) {
         dataSource.close();
         if (trace) log.debug("Successfully stopped PooledConnectionFactory.");
      }
   }

   @Override
   public Connection getConnection() throws PersistenceException {
      try {
         logBefore(true);
         Connection connection = dataSource.getConnection();
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
      return dataSource.getConfiguration().connectionPoolConfiguration().maxSize();
   }


   public long getActiveConnections() {
      return dataSource.getMetrics().activeCount();
   }

   private void logBefore(boolean checkout) {
      log(null, checkout, true);
   }

   private void logAfter(Connection connection, boolean checkout) {
      log(connection, checkout, false);
   }

   private void log(Connection connection, boolean checkout, boolean before) {
      if (trace) {
         String stage = before ? "before" : "after";
         String operation = checkout ? "checkout" : "release";
         log.tracef("DataSource %s %s (Active Connections) : %d", stage, operation, getActiveConnections());

         if (connection != null)
            log.tracef("Connection %s : %s", operation, connection);
      }
   }
}
