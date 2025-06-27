package org.infinispan.persistence.jdbc.common.configuration;

import static org.infinispan.commons.configuration.attributes.IdentityAttributeCopier.identityCopier;

import java.util.Objects;

import javax.sql.DataSource;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.persistence.jdbc.common.connectionfactory.ConnectionFactory;
import org.infinispan.persistence.jdbc.common.impl.connectionfactory.ManagedConnectionFactory;

/**
 * ManagedConnectionFactoryConfiguration.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
@BuiltBy(ManagedConnectionFactoryConfigurationBuilder.class)
public class ManagedConnectionFactoryConfiguration implements ConnectionFactoryConfiguration {

   public static final AttributeDefinition<String> JNDI_URL = AttributeDefinition.builder(org.infinispan.persistence.jdbc.common.configuration.Attribute.JNDI_URL, null, String.class).immutable().build();
   public static final AttributeDefinition<DataSource> DATA_SOURCE = AttributeDefinition.builder("dataSource", null, DataSource.class).copier(identityCopier()).autoPersist(false).immutable().build();

   public static AttributeSet attributeSet() {
      return new AttributeSet(ManagedConnectionFactoryConfiguration.class, JNDI_URL, DATA_SOURCE);
   }

   private final Attribute<String> jndiUrl;
   private final Attribute<DataSource> dataSource;
   private final AttributeSet attributes;

   ManagedConnectionFactoryConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.jndiUrl = attributes.attribute(JNDI_URL);
      this.dataSource = attributes.attribute(DATA_SOURCE);
   }

   public String jndiUrl() {
      return jndiUrl.get();
   }

   public DataSource dataSource() {
      return dataSource.get();
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

      return Objects.equals(attributes, that.attributes);
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
