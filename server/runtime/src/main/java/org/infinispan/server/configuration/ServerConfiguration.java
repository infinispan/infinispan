package org.infinispan.server.configuration;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.configuration.parsing.ParserScope;
import org.infinispan.configuration.serializing.SerializedWith;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.endpoint.EndpointsConfiguration;
import org.infinispan.server.configuration.security.SecurityConfiguration;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 */
@BuiltBy(ServerConfigurationBuilder.class)
@SerializedWith(value = ServerConfigurationSerializer.class, scope = ParserScope.GLOBAL)
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

   public Map<String, InterfaceConfiguration> networkInterfaces() {
      return interfaces.interfaces();
   }

   public Map<String, SocketBindingConfiguration> socketBindings() {
      return socketBindings.socketBindings();
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
