package org.infinispan.persistence.jdbc.common.configuration;

import static org.infinispan.persistence.jdbc.common.configuration.AbstractUnmanagedConnectionFactoryConfiguration.CONNECTION_URL;
import static org.infinispan.persistence.jdbc.common.configuration.AbstractUnmanagedConnectionFactoryConfiguration.DRIVER_CLASS;
import static org.infinispan.persistence.jdbc.common.configuration.AbstractUnmanagedConnectionFactoryConfiguration.PASSWORD;
import static org.infinispan.persistence.jdbc.common.configuration.AbstractUnmanagedConnectionFactoryConfiguration.USERNAME;
import static org.infinispan.persistence.jdbc.common.configuration.PooledConnectionFactoryConfiguration.PROPERTY_FILE;

import java.sql.Driver;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * PooledConnectionFactoryConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class PooledConnectionFactoryConfigurationBuilder<S extends AbstractJdbcStoreConfigurationBuilder<?, S>> extends AbstractJdbcStoreConfigurationChildBuilder<S>
      implements ConnectionFactoryConfigurationBuilder<PooledConnectionFactoryConfiguration> {

   private final AttributeSet attributes;

   protected PooledConnectionFactoryConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder<?, S> builder) {
      super(builder);
      attributes = PooledConnectionFactoryConfiguration.attributeSet();
   }

   public PooledConnectionFactoryConfigurationBuilder<S> propertyFile(String propertyFile) {
      attributes.attribute(PROPERTY_FILE).set(propertyFile);
      return this;
   }

   public PooledConnectionFactoryConfigurationBuilder<S> connectionUrl(String connectionUrl) {
      attributes.attribute(CONNECTION_URL).set(connectionUrl);
      return this;
   }

   public PooledConnectionFactoryConfigurationBuilder<S> driverClass(Class<? extends Driver> driverClass) {
      attributes.attribute(DRIVER_CLASS).set(driverClass.getName());
      return this;
   }

   public PooledConnectionFactoryConfigurationBuilder<S> driverClass(String driverClass) {
      attributes.attribute(DRIVER_CLASS).set(driverClass);
      return this;
   }

   public PooledConnectionFactoryConfigurationBuilder<S> username(String username) {
      attributes.attribute(USERNAME).set(username);
      return this;
   }

   public PooledConnectionFactoryConfigurationBuilder<S> password(String password) {
      attributes.attribute(PASSWORD).set(password);
      return this;
   }

   @Override
   public void validate() {
      // If a propertyFile is specified, then no exceptions are thrown for an incorrect config until the pool is created
      String propertyFile = attributes.attribute(PROPERTY_FILE).get();
      String connectionUrl = attributes.attribute(CONNECTION_URL).get();
      if (propertyFile == null && connectionUrl == null) {
         throw new CacheConfigurationException("Missing connectionUrl parameter");
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public PooledConnectionFactoryConfiguration create() {
      return new PooledConnectionFactoryConfiguration(attributes.protect());
   }

   @Override
   public PooledConnectionFactoryConfigurationBuilder<S> read(PooledConnectionFactoryConfiguration template) {
      attributes.read(template.attributes);
      return this;
   }

}
