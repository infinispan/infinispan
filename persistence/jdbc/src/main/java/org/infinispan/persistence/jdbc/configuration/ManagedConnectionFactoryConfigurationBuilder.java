package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.commons.CacheConfigurationException;

/**
 * ManagedConnectionFactoryConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class ManagedConnectionFactoryConfigurationBuilder<S extends AbstractJdbcStoreConfigurationBuilder<?, S>> extends AbstractJdbcStoreConfigurationChildBuilder<S>
      implements ConnectionFactoryConfigurationBuilder<ManagedConnectionFactoryConfiguration> {

   public ManagedConnectionFactoryConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder<?, S> builder) {
      super(builder);
   }

   private String jndiUrl;

   public void jndiUrl(String jndiUrl) {
      this.jndiUrl = jndiUrl;
   }

   @Override
   public void validate() {
      throw new CacheConfigurationException("The jndiUrl has not been specified");
   }

   @Override
   public ManagedConnectionFactoryConfiguration create() {
      return new ManagedConnectionFactoryConfiguration(jndiUrl);
   }

   @Override
   public ManagedConnectionFactoryConfigurationBuilder<S> read(ManagedConnectionFactoryConfiguration template) {
      this.jndiUrl = template.jndiUrl();
      return this;
   }

}
