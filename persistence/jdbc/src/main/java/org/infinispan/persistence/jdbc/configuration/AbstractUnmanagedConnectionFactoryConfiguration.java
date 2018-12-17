package org.infinispan.persistence.jdbc.configuration;

import org.infinispan.commons.configuration.attributes.Attribute;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public abstract class AbstractUnmanagedConnectionFactoryConfiguration implements ConnectionFactoryConfiguration {

   public static final AttributeDefinition<String> USERNAME = AttributeDefinition.builder("username", null, String.class).immutable().build();
   public static final AttributeDefinition<String> PASSWORD = AttributeDefinition.builder("password", null, String.class).immutable().build();
   public static final AttributeDefinition<String> DRIVER_CLASS = AttributeDefinition.builder("driverClass", null, String.class).xmlName("driver").immutable().build();
   public static final AttributeDefinition<String> CONNECTION_URL = AttributeDefinition.builder("connectionUrl", null, String.class).immutable().build();

   private final Attribute<String> connectionUrl;
   private final Attribute<String> driverClass;
   private final Attribute<String> username;
   private final Attribute<String> password;

   protected AttributeSet attributes;

   public static AttributeSet attributeSet() {
      return new AttributeSet(AbstractUnmanagedConnectionFactoryConfiguration.class, USERNAME, PASSWORD, DRIVER_CLASS, CONNECTION_URL);
   }

   public AbstractUnmanagedConnectionFactoryConfiguration(AttributeSet attributes) {
      this.attributes = attributes.checkProtection();
      this.connectionUrl = attributes.attribute(CONNECTION_URL);
      this.driverClass = attributes.attribute(DRIVER_CLASS);
      this.username = attributes.attribute(USERNAME);
      this.password = attributes.attribute(PASSWORD);
   }

   public String connectionUrl() {
      return connectionUrl.get();
   }

   public String driverClass() {
      return driverClass.get();
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

      return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
   }

   @Override
   public int hashCode() {
      return attributes != null ? attributes.hashCode() : 0;
   }
}
