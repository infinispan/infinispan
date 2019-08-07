package org.infinispan.server.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.configuration.security.SecurityConfiguration;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.network.NetworkAddress;
import org.infinispan.server.network.SocketBinding;
import org.infinispan.server.router.configuration.SinglePortRouterConfiguration;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 */
@BuiltBy(ServerConfigurationBuilder.class)
public class ServerConfiguration implements ConfigurationInfo {
   private final InterfacesConfiguration interfaces;
   private final SocketBindingsConfiguration socketBindings;
   private final List<ProtocolServerConfiguration> connectors;
   private final SinglePortRouterConfiguration endpoint;

   private final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.SERVER.toString(), true, false);
   private final List<ConfigurationInfo> elements = new ArrayList<>();

   ServerConfiguration(
         InterfacesConfiguration interfaces,
         SocketBindingsConfiguration socketBindings,
         SecurityConfiguration security,
         List<ProtocolServerConfiguration> connectors,
         SinglePortRouterConfiguration endpoint) {
      this.interfaces = interfaces;
      this.socketBindings = socketBindings;
      this.connectors = connectors;
      this.endpoint = endpoint;
      elements.add(interfaces);
      elements.add(socketBindings);
      elements.add(security);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return elements;
   }

   public Map<String, NetworkAddress> networkInterfaces() {
      return interfaces.getAddressMap();
   }

   public Map<String, SocketBinding> socketBindings() {
      return socketBindings.socketBindings().stream().collect(Collectors.toMap(SocketBindingConfiguration::name, SocketBindingConfiguration::getSocketBinding));
   }

   public List<ProtocolServerConfiguration> connectors() {
      return connectors;
   }

   public SinglePortRouterConfiguration endpoint() {
      return endpoint;
   }
}
