package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.persistence.jdbc.configuration.Element.SIMPLE_CONNECTION;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.connectionfactory.SimpleConnectionFactory;

/**
 * SimpleConnectionFactoryConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@BuiltBy(SimpleConnectionFactoryConfigurationBuilder.class)
public class SimpleConnectionFactoryConfiguration extends AbstractUnmanagedConnectionFactoryConfiguration {

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(SIMPLE_CONNECTION.getLocalName());

   SimpleConnectionFactoryConfiguration(AttributeSet attributes) {
      super(attributes);
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public Class<? extends ConnectionFactory> connectionFactoryClass() {
      return SimpleConnectionFactory.class;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SimpleConnectionFactoryConfiguration that = (SimpleConnectionFactoryConfiguration) o;

      return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
   }

   @Override
   public int hashCode() {
      return attributes != null ? attributes.hashCode() : 0;
   }

   @Override
   public String toString() {
      return "SimpleConnectionFactoryConfiguration [" + "attributes=" + attributes + "]";
   }
}
