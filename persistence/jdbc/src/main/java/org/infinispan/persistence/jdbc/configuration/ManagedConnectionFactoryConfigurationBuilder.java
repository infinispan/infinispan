package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.persistence.jdbc.configuration.ManagedConnectionFactoryConfiguration.JNDI_URL;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.ConfigurationBuilderInfo;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.configuration.global.GlobalConfiguration;

/**
 * ManagedConnectionFactoryConfigurationBuilder.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class ManagedConnectionFactoryConfigurationBuilder<S extends AbstractJdbcStoreConfigurationBuilder<?, S>> extends AbstractJdbcStoreConfigurationChildBuilder<S>
      implements ConnectionFactoryConfigurationBuilder<ManagedConnectionFactoryConfiguration>, ConfigurationBuilderInfo {

   private final AttributeSet attributes;

   public ManagedConnectionFactoryConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder<?, S> builder) {
      super(builder);
      attributes = ManagedConnectionFactoryConfiguration.attributeSet();
   }

   public void jndiUrl(String jndiUrl) {
      attributes.attribute(JNDI_URL).set(jndiUrl);
   }

   @Override
   public void validate() {
      String jndiUrl = attributes.attribute(JNDI_URL).get();
      if (jndiUrl == null) {
         throw new CacheConfigurationException("The jndiUrl has not been specified");
      }
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ManagedConnectionFactoryConfiguration.ELEMENT_DEFINITION;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public void validate(GlobalConfiguration globalConfig) {
   }

   @Override
   public ManagedConnectionFactoryConfiguration create() {
      return new ManagedConnectionFactoryConfiguration(attributes.protect());
   }

   @Override
   public ManagedConnectionFactoryConfigurationBuilder<S> read(ManagedConnectionFactoryConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

}
