package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.connectionfactory.PooledConnectionFactory;

@BuiltBy(PooledConnectionFactoryConfigurationBuilder.class)
public class PooledConnectionFactoryConfiguration extends AbstractUnmanagedConnectionFactoryConfiguration {

   public static final AttributeDefinition<String> PROPERTY_FILE = AttributeDefinition.builder(org.infinispan.persistence.jdbc.configuration.Attribute.PROPERTIES_FILE, null, String.class).immutable().build();

   public static AttributeSet attributeSet() {
      return new AttributeSet(PooledConnectionFactoryConfiguration.class, AbstractUnmanagedConnectionFactoryConfiguration.attributeSet(), PROPERTY_FILE);
   }

   private final Attribute<String> propertyFile;

   protected PooledConnectionFactoryConfiguration(AttributeSet attributes) {
      super(attributes);
      this.propertyFile = attributes.attribute(PROPERTY_FILE);
   }

   public String propertyFile() {
      return propertyFile.get();
   }

   @Override
   public Class<? extends ConnectionFactory> connectionFactoryClass() {
      return PooledConnectionFactory.class;
   }

   public AttributeSet attributes() {
      return attributes;
   }
}
