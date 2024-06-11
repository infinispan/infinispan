package org.infinispan.client.hotrod.test;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Set;

import org.infinispan.client.hotrod.FailoverRequestBalancingStrategy;
import org.infinispan.server.hotrod.HotRodServer;

/**
 * @since 9.4
 */
public class FixedServerBalancing implements FailoverRequestBalancingStrategy {
   private final HotRodServer server;

   public FixedServerBalancing(HotRodServer server) {
      this.server = server;
   }

   @Override
   public void setServers(Collection<SocketAddress> servers1) {
   }

   @Override
   public SocketAddress nextServer(Set<SocketAddress> failedServers) {
      return InetSocketAddress.createUnresolved(server.getHost(), server.getPort());
   }
}
