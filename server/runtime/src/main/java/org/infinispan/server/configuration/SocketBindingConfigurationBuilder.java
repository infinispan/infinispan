package org.infinispan.server.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;

public class SocketBindingConfigurationBuilder implements Builder<SocketBindingConfiguration> {

   private final AttributeSet attributes;
   private final ServerConfigurationBuilder server;

   SocketBindingConfigurationBuilder(ServerConfigurationBuilder server) {
      this.server = server;
      attributes = SocketBindingConfiguration.attributeDefinitionSet();
   }

   public SocketBindingConfigurationBuilder binding(String name, int port, String interfaceName) {
      attributes.attribute(SocketBindingConfiguration.NAME).set(name);
      attributes.attribute(SocketBindingConfiguration.PORT).set(port);
      attributes.attribute(SocketBindingConfiguration.INTERFACE).set(interfaceName);
      return this;
   }

   String interfaceName() {
      return attributes.attribute(SocketBindingConfiguration.INTERFACE).get();
   }

   @Override
   public SocketBindingConfiguration create() {
      throw new UnsupportedOperationException();
   }

   @Override
   public SocketBindingConfigurationBuilder read(SocketBindingConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

   public SocketBindingConfiguration create(InterfaceConfiguration interfaceConfiguration) {
      return new SocketBindingConfiguration(attributes.protect(), interfaceConfiguration);
   }
}
