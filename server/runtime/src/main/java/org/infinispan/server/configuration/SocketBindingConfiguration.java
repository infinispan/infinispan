package org.infinispan.server.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.network.SocketBinding;

public class SocketBindingConfiguration extends ConfigurationElement<SocketBindingConfiguration> {

   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, null, String.class).build();
   static final AttributeDefinition<Integer> PORT = AttributeDefinition.builder(Attribute.PORT, null, Integer.class).build();
   static final AttributeDefinition<String> INTERFACE = AttributeDefinition.builder(Attribute.INTERFACE, null, String.class).build();

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SocketBindingConfiguration.class, NAME, PORT, INTERFACE);
   }

   private final SocketBinding socketBinding;

   SocketBindingConfiguration(AttributeSet attributes, SocketBinding socketBinding) {
      super(Element.SOCKET_BINDING, attributes);
      this.socketBinding = socketBinding;
   }

   SocketBinding getSocketBinding() {
      return socketBinding;
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
}
