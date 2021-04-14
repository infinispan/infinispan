package org.infinispan.server.configuration;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.configuration.serializing.SerializedWith;
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
@SerializedWith(ServerConfigurationSerializer.class)
public class ServerConfiguration {
   final InterfacesConfiguration interfaces;
   final SocketBindingsConfiguration socketBindings;
   final SecurityConfiguration security;
   final DataSourcesConfiguration dataSources;
   final EndpointsConfiguration endpoints;
   private Server server;

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
