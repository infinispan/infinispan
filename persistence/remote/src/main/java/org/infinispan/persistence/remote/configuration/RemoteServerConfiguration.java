package org.infinispan.persistence.remote.configuration;

import static org.infinispan.persistence.remote.configuration.Element.SERVER;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

public class RemoteServerConfiguration implements ConfigurationInfo {

   static final AttributeDefinition<String> HOST = AttributeDefinition.builder("host", null, String.class).immutable().build();
   static final AttributeDefinition<Integer> PORT = AttributeDefinition.builder("port", 11222).immutable().build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(RemoteServerConfiguration.class, HOST, PORT);
   }

   static ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(SERVER.getLocalName());

   private final AttributeSet attributes;

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

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

      return attributes != null ? attributes.equals(that.attributes) : that.attributes == null;
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }
}
