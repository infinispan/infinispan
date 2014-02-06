package org.infinispan.spring.mock;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Set;

import org.infinispan.client.hotrod.impl.transport.tcp.RequestBalancingStrategy;

public final class MockRequestBalancingStrategy implements RequestBalancingStrategy {

   @Override
   public void setServers(final Collection<SocketAddress> servers) {
   }

   @Override
   public SocketAddress nextServer(Set<SocketAddress> failedServers) {
      return null;
   }
}
