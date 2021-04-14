package org.infinispan.server.configuration;

import java.util.List;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

public class SocketBindingsConfiguration extends ConfigurationElement<SocketBindingsConfiguration> {

   static final AttributeDefinition<Integer> PORT_OFFSET = AttributeDefinition.builder(Attribute.PORT_OFFSET, null, Integer.class).build();
   static final AttributeDefinition<String> DEFAULT_INTERFACE = AttributeDefinition.builder(Attribute.DEFAULT_INTERFACE, null, String.class).build();

   private final List<SocketBindingConfiguration> socketBindings;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SocketBindingsConfiguration.class, PORT_OFFSET, DEFAULT_INTERFACE);
   }

   SocketBindingsConfiguration(AttributeSet attributes, List<SocketBindingConfiguration> socketBindings) {
      super(Element.SOCKET_BINDINGS, attributes);
      this.socketBindings = socketBindings;
   }

   public Integer offset() {
      return attributes.attribute(SocketBindingsConfiguration.PORT_OFFSET).get();
   }

   List<SocketBindingConfiguration> socketBindings() {
      return socketBindings;
   }
}
