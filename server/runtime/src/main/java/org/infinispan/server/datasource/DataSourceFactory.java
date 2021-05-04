package org.infinispan.server.datasource;

import java.sql.SQLException;
import java.time.Duration;

import javax.sql.DataSource;

import org.infinispan.commons.util.ReflectionUtil;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.DataSourceConfiguration;

import io.agroal.api.AgroalDataSource;
import io.agroal.api.configuration.AgroalConnectionPoolConfiguration;
import io.agroal.api.configuration.supplier.AgroalConnectionFactoryConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalConnectionPoolConfigurationSupplier;
import io.agroal.api.configuration.supplier.AgroalDataSourceConfigurationSupplier;
import io.agroal.api.security.NamePrincipal;
import io.agroal.api.security.SimplePassword;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class DataSourceFactory {

   public static DataSource create(DataSourceConfiguration configuration) throws SQLException, ClassNotFoundException {
      AgroalConnectionFactoryConfigurationSupplier factory = new AgroalConnectionFactoryConfigurationSupplier();
      Class<?> driverClass = ReflectionUtil.getClassForName(configuration.driver(), Thread.currentThread().getContextClassLoader());
      factory.connectionProviderClass(driverClass)
            .jdbcTransactionIsolation(configuration.transactionIsolation())
            .jdbcUrl(configuration.url())
            .principal(new NamePrincipal(configuration.username()))
            .credential(new SimplePassword(configuration.password()))
            .initialSql(configuration.initialSql());
      configuration.connectionProperties().forEach(factory::jdbcProperty);

      AgroalConnectionPoolConfigurationSupplier pool = new AgroalConnectionPoolConfigurationSupplier();
      pool.connectionFactoryConfiguration(factory)
            .maxSize(configuration.maxSize())
            .minSize(configuration.minSize())
            .initialSize(configuration.initialSize())
            .connectionValidator(AgroalConnectionPoolConfiguration.ConnectionValidator.defaultValidator())
            .idleValidationTimeout(Duration.ofMillis(configuration.validateOnAcquisition()))
            .acquisitionTimeout(Duration.ofMillis(configuration.blockingTimeout()))
            .validationTimeout(Duration.ofMillis(configuration.backgroundValidation()))
            .leakTimeout(Duration.ofMillis(configuration.leakDetection()))
            .reapTimeout(Duration.ofMinutes(configuration.idleRemoval()));


      AgroalDataSourceConfigurationSupplier cs = new AgroalDataSourceConfigurationSupplier();
      cs.metricsEnabled(configuration.statistics())
            .connectionPoolConfiguration(pool);
      Server.log.dataSourceCreated(configuration.name(), configuration.jndiName());

      return AgroalDataSource.from(cs);
   }
}
