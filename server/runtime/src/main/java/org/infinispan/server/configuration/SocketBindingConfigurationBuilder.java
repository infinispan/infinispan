package org.infinispan.server.configuration;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.server.Server;
import org.infinispan.server.network.NetworkAddress;
import org.infinispan.server.network.SocketBinding;

public class SocketBindingConfigurationBuilder implements Builder<SocketBindingConfiguration> {

   private final AttributeSet attributes;
   private final ServerConfigurationBuilder server;
   private SocketBinding socketBinding;

   SocketBindingConfigurationBuilder(ServerConfigurationBuilder server) {
      this.server = server;
      attributes = SocketBindingConfiguration.attributeDefinitionSet();
   }

   public SocketBindingConfigurationBuilder binding(String name, int port, String interfaceName) {
      attributes.attribute(SocketBindingConfiguration.NAME).set(name);
      attributes.attribute(SocketBindingConfiguration.PORT).set(port);
      if (!server.interfaces().exists(interfaceName)) {
         throw Server.log.unknownInterface(interfaceName);
      }
      attributes.attribute(SocketBindingConfiguration.INTERFACE).set(interfaceName);
      NetworkAddress networkAddress = server.interfaces().getNetworkAddress(interfaceName);
      int offset = server.socketBindings().offset();
      this.socketBinding = new SocketBinding(name, networkAddress, port + offset);
      return this;
   }

   SocketBinding getSocketBinding() {
      return socketBinding;
   }

   @Override
   public void validate() {
   }

   @Override
   public SocketBindingConfiguration create() {
      return new SocketBindingConfiguration(attributes.protect(), socketBinding);
   }

   @Override
   public SocketBindingConfigurationBuilder read(SocketBindingConfiguration template) {
      this.attributes.read(template.attributes());
      return this;
   }

}
