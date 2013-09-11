package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.connectionfactory.ManagedConnectionFactory;

/**
 * ManagedConnectionFactoryConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@BuiltBy(ManagedConnectionFactoryConfigurationBuilder.class)
public class ManagedConnectionFactoryConfiguration implements ConnectionFactoryConfiguration {
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
   public String toString() {
      return "ManagedConnectionFactoryConfiguration [jndiUrl=" + jndiUrl + "]";
   }

}
