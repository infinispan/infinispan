package org.infinispan.client.hotrod.event;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collection;
import java.util.Set;

import org.infinispan.client.hotrod.FailoverRequestBalancingStrategy;
import org.infinispan.client.hotrod.impl.transport.tcp.RoundRobinBalancingStrategy;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

public class StickyServerLoadBalancingStrategy implements FailoverRequestBalancingStrategy {
   static Log log = LogFactory.getLog(StickyServerLoadBalancingStrategy.class);
   private InetSocketAddress stickyServer;
   private final RoundRobinBalancingStrategy delegate = new RoundRobinBalancingStrategy();

   @Override
   public void setServers(Collection<SocketAddress> servers) {
      log.info("Set servers: " + servers);
      delegate.setServers(servers);
      stickyServer = (InetSocketAddress) servers.iterator().next();
   }

   @Override
   public SocketAddress nextServer(Set<SocketAddress> failedServers) {
      if (failedServers != null && !failedServers.isEmpty())
         return delegate.nextServer(failedServers);
      else {
         log.info("Select " + stickyServer + " for load balancing");
         return stickyServer;
      }
   }

}
