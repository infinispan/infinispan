package org.infinispan.server.configuration;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;

public class SocketBindingConfiguration extends ConfigurationElement<SocketBindingConfiguration> {

   static final AttributeDefinition<String> NAME = AttributeDefinition.builder(Attribute.NAME, null, String.class).build();
   static final AttributeDefinition<Integer> PORT = AttributeDefinition.builder(Attribute.PORT, null, Integer.class).build();
   static final AttributeDefinition<String> INTERFACE = AttributeDefinition.builder(Attribute.INTERFACE, null, String.class).build();
   private final InterfaceConfiguration interfaceConfiguration;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SocketBindingConfiguration.class, NAME, PORT, INTERFACE);
   }

   SocketBindingConfiguration(AttributeSet attributes, InterfaceConfiguration interfaceConfiguration) {
      super(Element.SOCKET_BINDING, attributes);
      this.interfaceConfiguration = interfaceConfiguration;
   }

   public String name() {
      return attributes.attribute(NAME).get();
   }

   public String interfaceName() {
      return attributes.attribute(INTERFACE).get();
   }

   public int port() {
      return attributes.attribute(PORT).get();
   }

   public InterfaceConfiguration interfaceConfiguration() {
      return interfaceConfiguration;
   }
}
