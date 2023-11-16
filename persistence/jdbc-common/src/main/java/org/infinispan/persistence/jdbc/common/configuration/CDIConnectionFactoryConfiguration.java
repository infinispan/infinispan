package org.infinispan.persistence.jdbc.common.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.impl.connectionfactory.CDIConnectionFactory;

/**
 * ManagedConnectionFactoryConfiguration.
 *
 * @author Tristan Tarrant
 * @since 15.0
 */
@BuiltBy(CDIConnectionFactoryConfigurationBuilder.class)
public class CDIConnectionFactoryConfiguration extends ConfigurationElement<CDIConnectionFactoryConfiguration> implements ConnectionFactoryConfiguration {

   public static final AttributeDefinition<String> NAME = AttributeDefinition.builder(org.infinispan.persistence.jdbc.common.configuration.Attribute.NAME, null, String.class).immutable().build();
   public static final AttributeDefinition<String> ANNOTATION = AttributeDefinition.builder(org.infinispan.persistence.jdbc.common.configuration.Attribute.ANNOTATION, "io.quarkus.agroal.DataSource.DataSourceLiteral", String.class).immutable().build();

   public static AttributeSet attributeSet() {
      return new AttributeSet(CDIConnectionFactoryConfiguration.class, ANNOTATION, NAME);
   }

   CDIConnectionFactoryConfiguration(AttributeSet attributes) {
      super("", attributes);
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   public String annotation() {
      return attributes.attribute(ANNOTATION).get();
   }

   @Override
   public Class<? extends ConnectionFactory> connectionFactoryClass() {
      return CDIConnectionFactory.class;
   }
}
