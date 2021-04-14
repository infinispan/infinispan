package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.persistence.jdbc.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.impl.connectionfactory.ManagedConnectionFactory;

/**
 * ManagedConnectionFactoryConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@BuiltBy(ManagedConnectionFactoryConfigurationBuilder.class)
public class ManagedConnectionFactoryConfiguration implements ConnectionFactoryConfiguration {

   public static final AttributeDefinition<String> JNDI_URL = AttributeDefinition.builder(org.infinispan.persistence.jdbc.configuration.Attribute.JNDI_URL, null, String.class).immutable().build();

   public static AttributeSet attributeSet() {
      return new AttributeSet(ManagedConnectionFactoryConfiguration.class, JNDI_URL);
   }

   private final Attribute<String> jndiUrl;
   private final AttributeSet attributes;

   ManagedConnectionFactoryConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.jndiUrl = attributes.attribute(JNDI_URL);
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

   public AttributeSet attributes() {
      return attributes;
   }
}
