package org.infinispan.server.configuration;

import java.util.ArrayList;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

public class SocketBindingsConfiguration implements ConfigurationInfo {

   static final AttributeDefinition<Integer> PORT_OFFSET = AttributeDefinition.builder("portOffset", null, Integer.class).build();
   static final AttributeDefinition<String> DEFAULT_INTERFACE = AttributeDefinition.builder("defaultInterface", null, String.class).build();

   private static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.SOCKET_BINDINGS.toString());

   private final List<SocketBindingConfiguration> socketBindings;
   private final List<ConfigurationInfo> configs = new ArrayList<>();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SocketBindingsConfiguration.class, PORT_OFFSET, DEFAULT_INTERFACE);
   }

   private final AttributeSet attributes;

   SocketBindingsConfiguration(AttributeSet attributes, List<SocketBindingConfiguration> socketBindings) {
      this.attributes = attributes.checkProtection();
      this.socketBindings = socketBindings;
      this.configs.addAll(socketBindings);
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return configs;
   }

   @Override
   public AttributeSet attributes() {
      return attributes;
   }

   public Integer offset() {
      return attributes.attribute(SocketBindingsConfiguration.PORT_OFFSET).get();
   }

   List<SocketBindingConfiguration> socketBindings() {
      return socketBindings;
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SocketBindingsConfiguration that = (SocketBindingsConfiguration) o;

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
