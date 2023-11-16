package org.infinispan.persistence.jdbc.common.configuration;

import static org.infinispan.persistence.jdbc.common.configuration.CDIConnectionFactoryConfiguration.NAME;

import org.infinispan.commons.configuration.Combine;
import org.infinispan.commons.configuration.attributes.AttributeSet;

/**
 * CDIConnectionFactoryConfigurationBuilder
 *
 * @since 15.0
 */
public class CDIConnectionFactoryConfigurationBuilder<S extends AbstractJdbcStoreConfigurationBuilder<?, S>> extends AbstractJdbcStoreConfigurationChildBuilder<S>
      implements ConnectionFactoryConfigurationBuilder<CDIConnectionFactoryConfiguration> {

   private final AttributeSet attributes;

   public CDIConnectionFactoryConfigurationBuilder(AbstractJdbcStoreConfigurationBuilder<?, S> builder) {
      super(builder);
      attributes = CDIConnectionFactoryConfiguration.attributeSet();
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public void name(String name) {
      attributes.attribute(NAME).set(name);
   }

   @Override
   public CDIConnectionFactoryConfiguration create() {
      return new CDIConnectionFactoryConfiguration(attributes.protect());
   }

   @Override
   public CDIConnectionFactoryConfigurationBuilder<S> read(CDIConnectionFactoryConfiguration template, Combine combine) {
      this.attributes.read(template.attributes(), combine);
      return this;
   }
}
