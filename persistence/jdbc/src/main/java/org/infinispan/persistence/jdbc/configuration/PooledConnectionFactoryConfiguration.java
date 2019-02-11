package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.persistence.jdbc.configuration.Element.CONNECTION_POOL;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.connectionfactory.PooledConnectionFactory;

@BuiltBy(PooledConnectionFactoryConfigurationBuilder.class)
public class PooledConnectionFactoryConfiguration extends AbstractUnmanagedConnectionFactoryConfiguration {

   public static final AttributeDefinition<String> PROPERTY_FILE = AttributeDefinition.builder("propertyFile", null, String.class).immutable().build();

   public static AttributeSet attributeSet() {
      return new AttributeSet(PooledConnectionFactoryConfiguration.class, AbstractUnmanagedConnectionFactoryConfiguration.attributeSet(), PROPERTY_FILE);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(CONNECTION_POOL.getLocalName());

   private final Attribute<String> propertyFile;

   protected PooledConnectionFactoryConfiguration(AttributeSet attributes) {
      super(attributes);
      this.propertyFile = attributes.attribute(PROPERTY_FILE);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public String propertyFile() {
      return propertyFile.get();
   }

   @Override
   public Class<? extends ConnectionFactory> connectionFactoryClass() {
      return PooledConnectionFactory.class;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }
}
