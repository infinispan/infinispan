package org.infinispan.persistence.jdbc.configuration;

import java.sql.Driver;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * SimpleConnectionFactoryBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class SimpleConnectionFactoryConfigurationBuilder<S extends AbstractJdbcStoreConfigurationBuilder<?, S>> extends AbstractJdbcStoreConfigurationChildBuilder<S>
      implements ConnectionFactoryConfigurationBuilder<SimpleConnectionFactoryConfiguration> {

   private String connectionUrl;
   private String driverClass;
   private String username;
   private String password;

   public SimpleConnectionFactoryConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder<?, S> builder) {
      super(builder);
   }

   public SimpleConnectionFactoryConfigurationBuilder<S> connectionUrl(String connectionUrl) {
      this.connectionUrl = connectionUrl;
      return this;
   }

   public SimpleConnectionFactoryConfigurationBuilder<S> driverClass(Class<? extends Driver> driverClass) {
      this.driverClass = driverClass.getName();
      return this;
   }

   public SimpleConnectionFactoryConfigurationBuilder<S> driverClass(String driverClass) {
      this.driverClass = driverClass;
      return this;
   }

   public SimpleConnectionFactoryConfigurationBuilder<S> username(String username) {
      this.username = username;
      return this;
   }

   public SimpleConnectionFactoryConfigurationBuilder<S> password(String password) {
      this.password = password;
      return this;
   }

   @Override
   public void validate() {
      if (connectionUrl == null) {
         throw new CacheConfigurationException("A connectionUrl has not been specified");
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public SimpleConnectionFactoryConfiguration create() {
      return new SimpleConnectionFactoryConfiguration(connectionUrl, driverClass, username, password);
   }

   @Override
   public SimpleConnectionFactoryConfigurationBuilder<S> read(SimpleConnectionFactoryConfiguration template) {
      this.connectionUrl = template.connectionUrl();
      this.driverClass = template.driverClass();
      this.username = template.username();
      this.password = template.password();

      return this;
   }

}
