package org.infinispan.persistence.jdbc.common.configuration;

import static org.infinispan.persistence.jdbc.common.configuration.SimpleConnectionFactoryConfiguration.CONNECTION_URL;
import static org.infinispan.persistence.jdbc.common.configuration.SimpleConnectionFactoryConfiguration.DRIVER_CLASS;
import static org.infinispan.persistence.jdbc.common.configuration.SimpleConnectionFactoryConfiguration.PASSWORD;
import static org.infinispan.persistence.jdbc.common.configuration.SimpleConnectionFactoryConfiguration.USERNAME;

import java.sql.Driver;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * SimpleConnectionFactoryBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class SimpleConnectionFactoryConfigurationBuilder<S extends AbstractJdbcStoreConfigurationBuilder<?, S>> extends AbstractJdbcStoreConfigurationChildBuilder<S>
      implements ConnectionFactoryConfigurationBuilder<SimpleConnectionFactoryConfiguration> {

   private final AttributeSet attributes;

   public SimpleConnectionFactoryConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder<?, S> builder) {
      super(builder);
      attributes = SimpleConnectionFactoryConfiguration.attributeSet();
   }

   public SimpleConnectionFactoryConfigurationBuilder<S> connectionUrl(String connectionUrl) {
      attributes.attribute(CONNECTION_URL).set(connectionUrl);
      return this;
   }

   public SimpleConnectionFactoryConfigurationBuilder<S> driverClass(Class<? extends Driver> driverClass) {
      attributes.attribute(DRIVER_CLASS).set(driverClass.getName());
      return this;
   }

   public SimpleConnectionFactoryConfigurationBuilder<S> driverClass(String driverClass) {
      attributes.attribute(DRIVER_CLASS).set(driverClass);
      return this;
   }

   public SimpleConnectionFactoryConfigurationBuilder<S> username(String username) {
      attributes.attribute(USERNAME).set(username);
      return this;
   }

   public SimpleConnectionFactoryConfigurationBuilder<S> password(String password) {
      attributes.attribute(PASSWORD).set(password);
      return this;
   }

   @Override
   public void validate() {
      String connectionUrl = attributes.attribute(CONNECTION_URL).get();
      if (connectionUrl == null) {
         throw new CacheConfigurationException("A connectionUrl has not been specified");
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public SimpleConnectionFactoryConfiguration create() {
      return new SimpleConnectionFactoryConfiguration(attributes.protect());
   }

   @Override
   public SimpleConnectionFactoryConfigurationBuilder<S> read(SimpleConnectionFactoryConfiguration template) {
      attributes.read(template.attributes());
      return this;
   }

}
