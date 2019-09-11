package org.infinispan.server.configuration;

import java.util.Arrays;
import java.util.List;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.endpoint.EndpointsConfigurationBuilder;
import org.infinispan.server.configuration.endpoint.SinglePortServerConfigurationBuilder;
import org.infinispan.server.configuration.security.SecurityConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.server.network.SocketBinding;
import org.infinispan.server.security.ServerSecurityRealm;

/**
 * @author Tristan Tarrant
 * @since 10.0
 */
public class ServerConfigurationBuilder implements Builder<ServerConfiguration> {
   private final GlobalConfigurationBuilder builder;

   private final InterfacesConfigurationBuilder interfaces = new InterfacesConfigurationBuilder();
   private final SocketBindingsConfigurationBuilder socketBindings = new SocketBindingsConfigurationBuilder(this);
   private final SecurityConfigurationBuilder security = new SecurityConfigurationBuilder();
   private final EndpointsConfigurationBuilder endpoints = new EndpointsConfigurationBuilder(this);

   public ServerConfigurationBuilder(GlobalConfigurationBuilder builder) {
      this.builder = builder;
   }

   public List<ProtocolServerConfigurationBuilder<?, ?>> connectors() {
      return endpoints.connectors();
   }

   public SinglePortServerConfigurationBuilder endpoint() {
      return endpoints.singlePort();
   }

   public SecurityConfigurationBuilder security() {
      return security;
   }

   public InterfacesConfigurationBuilder interfaces() {
      return interfaces;
   }

   public SocketBindingsConfigurationBuilder socketBindings() {
      return socketBindings;
   }

   public EndpointsConfigurationBuilder endpoints() {
      return endpoints;
   }

   @Override
   public void validate() {
      Arrays.asList(interfaces, socketBindings, security, endpoints).forEach(Builder::validate);
   }

   @Override
   public ServerConfiguration create() {
      return new ServerConfiguration(
            interfaces.create(),
            socketBindings.create(),
            security.create(),
            endpoints.create()
      );
   }

   @Override
   public Builder<?> read(ServerConfiguration template) {
      // Do nothing
      return this;
   }

   public ServerSecurityRealm getSecurityRealm(String name) {
      ServerSecurityRealm serverSecurityRealm = security.realms().getServerSecurityRealm(name);
      if (serverSecurityRealm == null) {
         throw Server.log.unknownSecurityDomain(name);
      }
      return serverSecurityRealm;
   }

   public boolean hasSSLContext(String name) {
      return security.realms().getSSLContext(name) != null;
   }

   public SSLContext getSSLContext(String name) {
      SSLContext sslContext = security.realms().getSSLContext(name);
      if (sslContext == null) {
         throw Server.log.unknownSecurityDomain(name);
      }
      return sslContext;
   }

   public void applySocketBinding(String bingingName, ProtocolServerConfigurationBuilder builder) {
      if (!socketBindings.exists(bingingName)) {
         throw Server.log.unknownSocketBinding(bingingName);
      }
      SocketBinding socketBinding = socketBindings.getSocketBinding(bingingName);
      String host = socketBinding.getAddress().getAddress().getHostAddress();
      int port = socketBinding.getPort();
      SinglePortServerConfigurationBuilder endpoint = endpoints().singlePort();
      if (builder != endpoint) {
         // Ensure we are using a different socket binding than the one used by the single-port endpoint
         if (endpoint.host().equals(host) && endpoint.port() == port) {
            throw Server.log.protocolCannotUseSameSocketBindingAsEndpoint();
         }
      }
      builder.host(host).port(port);
   }
}
