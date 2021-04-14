package org.infinispan.persistence.remote.configuration;

import java.util.Objects;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class RemoteServerConfiguration {

   static final AttributeDefinition<String> HOST = AttributeDefinition.builder(Attribute.HOST, null, String.class).immutable().build();
   static final AttributeDefinition<Integer> PORT = AttributeDefinition.builder(Attribute.PORT, 11222).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RemoteServerConfiguration.class, HOST, PORT);
   }

   private final AttributeSet attributes;

   RemoteServerConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
   }

   public AttributeSet attributes() {
      return attributes;
   }

   public String host() {
      return attributes.attribute(HOST).get();
   }

   public int port() {
      return attributes.attribute(PORT).get();
   }

   @Override
   public String toString() {
      return attributes.toString();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RemoteServerConfiguration that = (RemoteServerConfiguration) o;

      return Objects.equals(attributes, that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }
}
