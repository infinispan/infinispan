package org.infinispan.server.configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.net.ssl.SSLContext;

import org.infinispan.commons.configuration.Builder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.rest.authentication.Authenticator;
import org.infinispan.server.Server;
import org.infinispan.server.configuration.endpoint.SinglePortServerConfigurationBuilder;
import org.infinispan.server.core.configuration.ProtocolServerConfigurationBuilder;
import org.infinispan.server.core.security.ServerAuthenticationProvider;
import org.infinispan.server.network.NetworkAddress;
import org.infinispan.server.network.SocketBinding;
import org.infinispan.server.security.ElytronHTTPAuthenticator;
import org.infinispan.server.security.ElytronSASLAuthenticationProvider;
import org.wildfly.security.auth.server.SecurityDomain;

/**
 * @author Tristan Tarrant
 * @since 10.0
 */
public class ServerConfigurationBuilder implements Builder<ServerConfiguration> {
   private final Map<String, NetworkAddress> networkInterfaces = new HashMap<>(2);
   private final Map<String, SecurityDomain> securityDomains = new HashMap<>(2);
   private final Map<String, SSLContext> sslContexts = new HashMap<>(2);
   private final Map<String, SocketBinding> socketBindings = new HashMap<>(2);
   private final List<ProtocolServerConfigurationBuilder<?, ?>> connectors = new ArrayList<>(2);
   private final GlobalConfigurationBuilder builder;
   private final SinglePortServerConfigurationBuilder endpoint = new SinglePortServerConfigurationBuilder();

   public ServerConfigurationBuilder(GlobalConfigurationBuilder builder) {
      this.builder = builder;
   }

   public <T extends ProtocolServerConfigurationBuilder<?, ?>> T addConnector(Class<T> klass) {
      try {
         T builder = klass.getConstructor().newInstance();
         this.connectors.add(builder);
         this.endpoint.applyConfigurationToProtocol(builder);
         return builder;
      } catch (Exception e) {
         throw Server.log.cannotInstantiateProtocolServerConfigurationBuilder(klass, e);
      }
   }

   public List<ProtocolServerConfigurationBuilder<?, ?>> connectors() {
      return connectors;
   }

   public SinglePortServerConfigurationBuilder endpoint() {
      return endpoint;
   }

   public void addSecurityRealm(String name, SecurityDomain domain) {
      if (securityDomains.putIfAbsent(name, domain) != null) {
         throw Server.log.duplicateSecurityDomain(name);
      }
   }

   public void addSSLContext(String name, SSLContext sslContext) {
      if (sslContexts.putIfAbsent(name, sslContext) != null) {
         throw Server.log.duplicateSecurityDomain(name);
      }
   }

   public void addNetworkInterface(NetworkAddress networkAddress) {
      if (networkInterfaces.putIfAbsent(networkAddress.getName(), networkAddress) != null) {
         throw Server.log.duplicatePath(networkAddress.getName());
      }
   }

   public void addSocketBinding(String name, NetworkAddress networkAddress, int port) {
      if (socketBindings.putIfAbsent(name, new SocketBinding(name, networkAddress, port)) != null) {
         throw Server.log.duplicatePath(name);
      }
   }

   public void addSocketBinding(String name, String interfaceName, int port) {
      if (networkInterfaces.containsKey(interfaceName)) {
         addSocketBinding(name, networkInterfaces.get(interfaceName), port);
      } else {
         throw Server.log.unknownInterface(interfaceName);
      }
   }

   @Override
   public void validate() {
   }

   @Override
   public ServerConfiguration create() {
      return new ServerConfiguration(
            networkInterfaces,
            socketBindings,
            securityDomains,
            connectors.stream().map(b -> b.create()).collect(Collectors.toList()),
            endpoint.create()
      );
   }

   @Override
   public Builder<?> read(ServerConfiguration template) {
      // Do nothing
      return this;
   }

   public SocketBinding getSocketBinding(String name) {
      if (socketBindings.containsKey(name)) {
         return socketBindings.get(name);
      } else {
         throw Server.log.unknownSocketBinding(name);
      }
   }

   public SecurityDomain getSecurityRealm(String name) {
      if (securityDomains.containsKey(name)) {
         return securityDomains.get(name);
      } else {
         throw Server.log.unknownSecurityDomain(name);
      }
   }

   public ServerAuthenticationProvider getSASLAuthenticationProvider(String name) {
      return new ElytronSASLAuthenticationProvider(name, getSecurityRealm(name));
   }

   public Authenticator getHTTPAuthenticationProvider(String name) {
      return new ElytronHTTPAuthenticator(name, getSecurityRealm(name));
   }

   public SSLContext getSSLContext(String name) {
      if (sslContexts.containsKey(name)) {
         return sslContexts.get(name);
      } else {
         throw Server.log.unknownSecurityDomain(name);
      }
   }

   public void applySocketBinding(String name, ProtocolServerConfigurationBuilder builder) {
      SocketBinding socketBinding = socketBindings.get(name);
      if (socketBinding != null) {
         builder.host(socketBinding.getAddress().getAddress().getHostAddress()).port(socketBinding.getPort());
      } else {
         throw Server.log.unknownSocketBinding(name);
      }
   }
}
