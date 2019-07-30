package org.infinispan.server.configuration;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.server.core.configuration.ProtocolServerConfiguration;
import org.infinispan.server.network.NetworkAddress;
import org.infinispan.server.network.SocketBinding;
import org.infinispan.server.router.configuration.SinglePortRouterConfiguration;
import org.infinispan.server.security.ServerSecurityRealm;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 */
@BuiltBy(ServerConfigurationBuilder.class)
public class ServerConfiguration {
   private final Map<String, NetworkAddress> networkInterfaces;
   private final Map<String, SocketBinding> socketBindings;
   private final Map<String, ServerSecurityRealm> securityRealms;
   private final List<ProtocolServerConfiguration> connectors;
   private final SinglePortRouterConfiguration endpoint;

   public ServerConfiguration(
         Map<String, NetworkAddress> networkInterfaces,
         Map<String, SocketBinding> socketBindings,
         final Map<String, ServerSecurityRealm> securityRealms,
         List<ProtocolServerConfiguration> connectors,
         SinglePortRouterConfiguration endpoint
   ) {
      this.networkInterfaces = Collections.unmodifiableMap(networkInterfaces);
      this.socketBindings = Collections.unmodifiableMap(socketBindings);
      this.securityRealms = Collections.unmodifiableMap(securityRealms);
      this.connectors = connectors;
      this.endpoint = endpoint;
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

   public List<ProtocolServerConfiguration> connectors() {
      return connectors;
   }

   public SinglePortRouterConfiguration endpoint() {
      return endpoint;
   }
}
