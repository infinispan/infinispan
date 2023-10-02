package org.infinispan.persistence.jdbc.common.configuration;

import static org.infinispan.commons.configuration.attributes.AttributeSerializer.SECRET;

import java.util.Objects;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public abstract class AbstractUnmanagedConnectionFactoryConfiguration implements ConnectionFactoryConfiguration {

   public static final AttributeDefinition<String> USERNAME = AttributeDefinition.builder(org.infinispan.persistence.jdbc.common.configuration.Attribute.USERNAME, null, String.class).serializer(SECRET).immutable().build();
   public static final AttributeDefinition<String> PASSWORD = AttributeDefinition.builder(org.infinispan.persistence.jdbc.common.configuration.Attribute.PASSWORD, null, String.class).serializer(SECRET).immutable().build();
   public static final AttributeDefinition<String> DRIVER = AttributeDefinition.builder(org.infinispan.persistence.jdbc.common.configuration.Attribute.DRIVER, null, String.class).immutable().build();
   public static final AttributeDefinition<String> CONNECTION_URL = AttributeDefinition.builder(org.infinispan.persistence.jdbc.common.configuration.Attribute.CONNECTION_URL, null, String.class).immutable().build();

   private final Attribute<String> connectionUrl;
   private final Attribute<String> driver;
   private final Attribute<String> username;
   private final Attribute<String> password;

   protected AttributeSet attributes;

   public static AttributeSet attributeSet() {
      return new AttributeSet(AbstractUnmanagedConnectionFactoryConfiguration.class, USERNAME, PASSWORD, DRIVER, CONNECTION_URL);
   }

   public AbstractUnmanagedConnectionFactoryConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.connectionUrl = attributes.attribute(CONNECTION_URL);
      this.driver = attributes.attribute(DRIVER);
      this.username = attributes.attribute(USERNAME);
      this.password = attributes.attribute(PASSWORD);
   }

   public String connectionUrl() {
      return connectionUrl.get();
   }

   public String driverClass() {
      return driver.get();
   }

   public String username() {
      return username.get();
   }

   public String password() {
      return password.get();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AbstractUnmanagedConnectionFactoryConfiguration that = (AbstractUnmanagedConnectionFactoryConfiguration) o;

      return Objects.equals(attributes, that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes != null ? attributes.hashCode() : 0;
   }

   @Override
   public String toString() {
      return this.getClass().getName() +
            "attributes=" + attributes +
            '}';
   }
}
