package org.infinispan.spring.mock;

import org.infinispan.client.hotrod.impl.transport.tcp.FailoverRequestBalancingStrategy;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Set;

public final class MockRequestBalancingStrategy implements FailoverRequestBalancingStrategy {

   @Override
   public void setServers(final Collection<SocketAddress> servers) {
   }

   @Override
   public SocketAddress nextServer(Set<SocketAddress> failedServers) {
      return null;
   }

   @Override
   public SocketAddress nextServer() {
      return null;
   }

}
