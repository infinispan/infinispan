package org.infinispan.server.configuration;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.network.SocketBinding;

public class SocketBindingConfiguration implements ConfigurationInfo {

   static final AttributeDefinition<String> NAME = AttributeDefinition.builder("name", null, String.class).build();
   static final AttributeDefinition<Integer> PORT = AttributeDefinition.builder("port", null, Integer.class).build();
   static final AttributeDefinition<String> INTERFACE = AttributeDefinition.builder("interface", null, String.class).build();

   private static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.SOCKET_BINDING.toString());

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SocketBindingConfiguration.class, NAME, PORT, INTERFACE);
   }

   private final AttributeSet attributes;
   private final SocketBinding socketBinding;

   SocketBindingConfiguration(AttributeSet attributes, SocketBinding socketBinding) {
      this.attributes = attributes.checkProtection();
      this.socketBinding = socketBinding;
   }

   SocketBinding getSocketBinding() {
      return socketBinding;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   public String interfaceName() {
      return attributes.attribute(INTERFACE).get();
   }

   public Integer port() {
      return attributes.attribute(PORT).get();
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SocketBindingConfiguration that = (SocketBindingConfiguration) o;

      return attributes.equals(that.attributes);
   }

   @Override
   public int hashCode() {
      return attributes.hashCode();
   }

   @Override
   public String toString() {
      return "SocketBindingConfiguration{" +
            "attributes=" + attributes +
            '}';
   }
}
