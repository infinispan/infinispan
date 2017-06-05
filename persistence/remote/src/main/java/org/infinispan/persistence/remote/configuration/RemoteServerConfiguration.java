package org.infinispan.persistence.remote.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class RemoteServerConfiguration {

   static final AttributeDefinition<String> HOST = AttributeDefinition.builder("host", null, String.class).immutable().build();
   static final AttributeDefinition<Integer> PORT = AttributeDefinition.builder("port", 11222).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RemoteServerConfiguration.class, HOST, PORT);
   }

   private final AttributeSet attributes;

   RemoteServerConfiguration(AttributeSet attributes) {
      this.attributes = attributes;
   }

   AttributeSet attributes() {
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
      RemoteServerConfiguration other = (RemoteServerConfiguration) o;
      return attributes.equals(other.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }
}
