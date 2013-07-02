package org.infinispan.loaders.jdbc.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.loaders.jdbc.AbstractJdbcCacheStoreConfig;
import org.infinispan.loaders.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.loaders.jdbc.connectionfactory.ManagedConnectionFactory;

/**
 * ManagedConnectionFactoryConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@BuiltBy(ManagedConnectionFactoryConfigurationBuilder.class)
public class ManagedConnectionFactoryConfiguration implements ConnectionFactoryConfiguration, LegacyConnectionFactoryAdaptor {
   private final String jndiUrl;

   ManagedConnectionFactoryConfiguration(String jndiUrl) {
      this.jndiUrl = jndiUrl;
   }

   public String jndiUrl() {
      return jndiUrl;
   }

   @Override
   public Class<? extends ConnectionFactory> connectionFactoryClass() {
      return ManagedConnectionFactory.class;
   }

   @Override
   public void adapt(AbstractJdbcCacheStoreConfig config) {
      config.setConnectionFactoryClass(connectionFactoryClass().getName());
      config.setDatasourceJndiLocation(jndiUrl);
   }

   @Override
   public String toString() {
      return "ManagedConnectionFactoryConfiguration [jndiUrl=" + jndiUrl + "]";
   }

}
