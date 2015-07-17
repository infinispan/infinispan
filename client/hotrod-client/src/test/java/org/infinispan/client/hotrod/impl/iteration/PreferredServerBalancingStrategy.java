package org.infinispan.client.hotrod.impl.iteration;

import org.infinispan.client.hotrod.impl.transport.tcp.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Set;

public class PreferredServerBalancingStrategy implements FailoverRequestBalancingStrategy {

   private final InetSocketAddress preferredServer;
   private final RoundRobinBalancingStrategy roundRobinBalancingStrategy = new RoundRobinBalancingStrategy();

   public PreferredServerBalancingStrategy(InetSocketAddress preferredServer) {
      this.preferredServer = preferredServer;
   }

   @Override
   public void setServers(Collection<SocketAddress> servers) {
      roundRobinBalancingStrategy.setServers(servers);
   }

   @Override
   public SocketAddress nextServer() {
      return roundRobinBalancingStrategy.nextServer();
   }

   @Override
   public SocketAddress nextServer(Set<SocketAddress> failedServers) {
      if (failedServers != null && !failedServers.isEmpty() && failedServers.contains(preferredServer)) {
         return roundRobinBalancingStrategy.nextServer(failedServers);
      }
      return preferredServer;
   }
}
