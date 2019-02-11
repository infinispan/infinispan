package org.infinispan.persistence.jdbc.configuration;

import static org.infinispan.persistence.jdbc.configuration.Element.DATA_SOURCE;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.connectionfactory.ManagedConnectionFactory;

/**
 * ManagedConnectionFactoryConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@BuiltBy(ManagedConnectionFactoryConfigurationBuilder.class)
public class ManagedConnectionFactoryConfiguration implements ConnectionFactoryConfiguration, ConfigurationInfo {

   public static final AttributeDefinition<String> JNDI_URL = AttributeDefinition.builder("jndiUrl", null, String.class).immutable().build();

   public static AttributeSet attributeSet() {
      return new AttributeSet(ManagedConnectionFactoryConfiguration.class, JNDI_URL);
   }

   private final Attribute<String> jndiUrl;

   static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(DATA_SOURCE.getLocalName());
   private final AttributeSet attributes;

   ManagedConnectionFactoryConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.jndiUrl = attributes.attribute(JNDI_URL);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   public String jndiUrl() {
      return jndiUrl.get();
   }

   @Override
   public Class<? extends ConnectionFactory> connectionFactoryClass() {
      return ManagedConnectionFactory.class;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ManagedConnectionFactoryConfiguration that = (ManagedConnectionFactoryConfiguration) o;

      return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
   }

   @Override
   public String toString() {
      return "ManagedConnectionFactoryConfiguration [" +
            "attributes=" + attributes +
            ']';
   }

   @Override
   public int hashCode() {
      return attributes != null ? attributes.hashCode() : 0;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }
}
