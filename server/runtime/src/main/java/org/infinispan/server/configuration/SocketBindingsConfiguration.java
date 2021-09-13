package org.infinispan.server.configuration;

import java.util.Map;

import org.infinispan.commons.configuration.attributes.AttributeDefinition;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.configuration.attributes.ConfigurationElement;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.endpoint.SinglePortServerConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.server.network.NetworkAddress;

public class SocketBindingsConfiguration extends ConfigurationElement<SocketBindingsConfiguration> {

   static final AttributeDefinition<Integer> PORT_OFFSET = AttributeDefinition.builder(Attribute.PORT_OFFSET, null, Integer.class).build();
   static final AttributeDefinition<String> DEFAULT_INTERFACE = AttributeDefinition.builder(Attribute.DEFAULT_INTERFACE, null, String.class).build();

   private final Map<String, SocketBindingConfiguration> socketBindings;

   static AttributeSet attributeDefinitionSet() {
      return new AttributeSet(SocketBindingsConfiguration.class, PORT_OFFSET, DEFAULT_INTERFACE);
   }

   SocketBindingsConfiguration(AttributeSet attributes, Map<String, SocketBindingConfiguration> socketBindings) {
      super(Element.SOCKET_BINDINGS, attributes);
      this.socketBindings = socketBindings;
   }

   public Integer offset() {
      return attributes.attribute(SocketBindingsConfiguration.PORT_OFFSET).get();
   }

   Map<String, SocketBindingConfiguration> socketBindings() {
      return socketBindings;
   }

   public void applySocketBinding(String bindingName, ProtocolServerConfigurationBuilder builder, SinglePortServerConfigurationBuilder singlePort) {
      if (!socketBindings.containsKey(bindingName)) {
         throw Server.log.unknownSocketBinding(bindingName);
      }
      SocketBindingConfiguration binding = socketBindings.get(bindingName);
      NetworkAddress networkAddress = binding.interfaceConfiguration().getNetworkAddress();
      String host = networkAddress.getAddress().getHostAddress();
      int port = binding.port() + offset();
      if (builder != singlePort) {
         // Ensure we are using a different socket binding than the one used by the single-port endpoint
         if (builder.startTransport() && singlePort.host().equals(host) && singlePort.port() == port) {
            throw Server.log.protocolCannotUseSameSocketBindingAsEndpoint();
         }
      }
      builder.socketBinding(bindingName).host(host).port(port);
   }
}
