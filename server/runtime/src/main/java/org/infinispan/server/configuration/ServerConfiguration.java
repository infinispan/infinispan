package org.infinispan.server.configuration;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.endpoint.EndpointsConfiguration;
import org.infinispan.server.configuration.security.SecurityConfiguration;
import org.infinispan.server.network.NetworkAddress;
import org.infinispan.server.network.SocketBinding;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 */
@BuiltBy(ServerConfigurationBuilder.class)
public class ServerConfiguration implements ConfigurationInfo {
   private final InterfacesConfiguration interfaces;
   private final SocketBindingsConfiguration socketBindings;
   private final SecurityConfiguration security;
   private final DataSourcesConfiguration dataSources;
   private final EndpointsConfiguration endpoints;
   private Server server;

   private final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition(Element.SERVER.toString(), true, false);
   private final List<ConfigurationInfo> elements = new ArrayList<>();

   ServerConfiguration(
         InterfacesConfiguration interfaces,
         SocketBindingsConfiguration socketBindings,
         SecurityConfiguration security,
         DataSourcesConfiguration dataSources,
         EndpointsConfiguration endpoints) {
      this.interfaces = interfaces;
      this.socketBindings = socketBindings;
      this.security = security;
      this.dataSources = dataSources;
      this.endpoints = endpoints;
      elements.add(interfaces);
      elements.add(socketBindings);
      elements.add(security);
      elements.add(dataSources);
      elements.addAll(endpoints.subElements());
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


   public SecurityConfiguration security() {
      return security;
   }

   public Map<String, DataSourceConfiguration> dataSources() {
      return dataSources.dataSources().stream().collect(Collectors.toMap(DataSourceConfiguration::name, Function.identity()));
   }

   public EndpointsConfiguration endpoints() {
      return endpoints;
   }

   public Server getServer() {
      return server;
   }

   public void setServer(Server server) {
      this.server = server;
   }
}
