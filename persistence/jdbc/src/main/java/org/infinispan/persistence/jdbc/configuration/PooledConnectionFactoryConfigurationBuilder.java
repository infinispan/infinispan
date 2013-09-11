package org.infinispan.persistence.jdbc.configuration;

import java.sql.Driver;

import org.infinispan.commons.CacheConfigurationException;

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

   private String connectionUrl;
   private String driverClass;
   private String username;
   private String password;

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
      if (connectionUrl == null) {
         throw new CacheConfigurationException("Missing connectionUrl parameter");
      }
   }

   @Override
   public PooledConnectionFactoryConfiguration create() {
      return new PooledConnectionFactoryConfiguration(connectionUrl, driverClass, username, password);
   }

   @Override
   public PooledConnectionFactoryConfigurationBuilder<S> read(PooledConnectionFactoryConfiguration template) {
      this.connectionUrl = template.connectionUrl();
      this.driverClass = template.driverClass();
      this.username = template.username();
      this.password = template.password();
      return this;
   }

}
