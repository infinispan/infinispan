package org.infinispan.persistence.jdbc.common.configuration;

import static org.infinispan.persistence.jdbc.common.configuration.ManagedConnectionFactoryConfiguration.DATA_SOURCE;
import static org.infinispan.persistence.jdbc.common.configuration.ManagedConnectionFactoryConfiguration.JNDI_URL;
import static org.infinispan.persistence.jdbc.common.logging.Log.CONFIG;

import javax.sql.DataSource;

import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * ManagedConnectionFactoryConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class ManagedConnectionFactoryConfigurationBuilder<S extends AbstractJdbcStoreConfigurationBuilder<?, S>> extends AbstractJdbcStoreConfigurationChildBuilder<S>
      implements ConnectionFactoryConfigurationBuilder<ManagedConnectionFactoryConfiguration> {

   private final AttributeSet attributes;

   public ManagedConnectionFactoryConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder<?, S> builder) {
      super(builder);
      attributes = ManagedConnectionFactoryConfiguration.attributeSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public ManagedConnectionFactoryConfigurationBuilder<S> jndiUrl(String jndiUrl) {
      attributes.attribute(JNDI_URL).set(jndiUrl);
      return this;
   }

   public ManagedConnectionFactoryConfigurationBuilder<S> dataSource(DataSource dataSource) {
      attributes.attribute(DATA_SOURCE).set(dataSource);
      return this;
   }

   @Override
   public void validate() {
      String jndiUrl = attributes.attribute(JNDI_URL).get();
      DataSource dataSource = attributes.attribute(DATA_SOURCE).get();
      if (jndiUrl == null && dataSource == null) {
         throw CONFIG.jndiUrlOrDataSourceRequired();
      }
      if (jndiUrl != null && dataSource != null) {
         throw CONFIG.jndiUrlAndDataSourceSet();
      }
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public ManagedConnectionFactoryConfiguration create() {
      return new ManagedConnectionFactoryConfiguration(attributes.protect());
   }

   @Override
   public ManagedConnectionFactoryConfigurationBuilder<S> read(ManagedConnectionFactoryConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      return this;
   }
}
