package org.infinispan.server.server.configuration;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.server.network.NetworkAddress;
import org.infinispan.server.server.network.SocketBinding;
import org.infinispan.server.server.security.ServerSecurityRealm;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 */
@BuiltBy(ServerConfigurationBuilder.class)
public class ServerConfiguration {
   final Map<String, NetworkAddress> networkInterfaces;
   final Map<String, SocketBinding> socketBindings;
   final Map<String, ServerSecurityRealm> securityRealms;
   final List<ProtocolServerConfiguration> endpoints;

   public ServerConfiguration(
         Map<String, NetworkAddress> networkInterfaces,
         Map<String, SocketBinding> socketBindings,
         final Map<String, ServerSecurityRealm> securityRealms,
         List<ProtocolServerConfiguration> endpoints
   ) {
      this.networkInterfaces = Collections.unmodifiableMap(networkInterfaces);
      this.socketBindings = Collections.unmodifiableMap(socketBindings);
      this.securityRealms = Collections.unmodifiableMap(securityRealms);
      this.endpoints = endpoints;
   }

   public Map<String, NetworkAddress> networkInterfaces() {
      return networkInterfaces;
   }

   public Map<String, SocketBinding> socketBindings() {
      return socketBindings;
   }

   public Map<String, ServerSecurityRealm> securityRealms() {
      return securityRealms;
   }

   public List<ProtocolServerConfiguration> endpoints() {
      return endpoints;
   }
}
