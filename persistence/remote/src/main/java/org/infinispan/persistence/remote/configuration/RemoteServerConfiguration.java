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

   RemoteServerConfiguration(AttributeSet attibutes) {
      this.attributes = attibutes;
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
      return "RemoteServerConfiguration{" +
            "host='" + host() + '\'' +
            ", port=" + port() +
            '}';
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      RemoteServerConfiguration that = (RemoteServerConfiguration) o;

      if (port() != that.port()) return false;
      return host() != null ? host().equals(that.host()) : that.host() == null;

   }

   @Override
   public int hashCode() {
      int result = host() != null ? host().hashCode() : 0;
      result = 31 * result + port();
      return result;
   }
}
