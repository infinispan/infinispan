package org.infinispan.persistence.jdbc.configuration;

import java.sql.Driver;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * PooledConnectionFactoryConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class PooledConnectionFactoryConfigurationBuilder<S extends AbstractJdbcStoreConfigurationBuilder<?, S>> extends AbstractJdbcStoreConfigurationChildBuilder<S>
      implements ConnectionFactoryConfigurationBuilder<PooledConnectionFactoryConfiguration> {

   protected PooledConnectionFactoryConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder<?, S> builder) {
      super(builder);
   }

   private String propertyFile;
   private String connectionUrl;
   private String driverClass;
   private String username;
   private String password;

   public PooledConnectionFactoryConfigurationBuilder<S> propertyFile(String propertyFile) {
      this.propertyFile = propertyFile;
      return this;
   }

   public PooledConnectionFactoryConfigurationBuilder<S> connectionUrl(String connectionUrl) {
      this.connectionUrl = connectionUrl;
      return this;
   }

   public PooledConnectionFactoryConfigurationBuilder<S> driverClass(Class<? extends Driver> driverClass) {
      this.driverClass = driverClass.getName();
      return this;
   }

   public PooledConnectionFactoryConfigurationBuilder<S> driverClass(String driverClass) {
      this.driverClass = driverClass;
      return this;
   }

   public PooledConnectionFactoryConfigurationBuilder<S> username(String username) {
      this.username = username;
      return this;
   }

   public PooledConnectionFactoryConfigurationBuilder<S> password(String password) {
      this.password = password;
      return this;
   }

   @Override
   public void validate() {
      // If a propertyFile is specified, then no exceptions are thrown for an incorrect config until the pool is created
      if (propertyFile == null && connectionUrl == null) {
         throw new CacheConfigurationException("Missing connectionUrl parameter");
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public PooledConnectionFactoryConfiguration create() {
      return new PooledConnectionFactoryConfiguration(propertyFile, connectionUrl, driverClass, username, password);
   }

   @Override
   public PooledConnectionFactoryConfigurationBuilder<S> read(PooledConnectionFactoryConfiguration template) {
      this.propertyFile = template.propertyFile();
      this.connectionUrl = template.connectionUrl();
      this.driverClass = template.driverClass();
      this.username = template.username();
      this.password = template.password();
      return this;
   }

}
