package org.infinispan.persistence.jdbc.common.configuration;

import java.util.Objects;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.impl.connectionfactory.SimpleConnectionFactory;

/**
 * SimpleConnectionFactoryConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@BuiltBy(SimpleConnectionFactoryConfigurationBuilder.class)
public class SimpleConnectionFactoryConfiguration extends AbstractUnmanagedConnectionFactoryConfiguration {

   SimpleConnectionFactoryConfiguration(AttributeSet attributes) {
      super(attributes);
   }

   public AttributeSet attributes() {
      return attributes;
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

      return Objects.equals(attributes, that.attributes);
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
