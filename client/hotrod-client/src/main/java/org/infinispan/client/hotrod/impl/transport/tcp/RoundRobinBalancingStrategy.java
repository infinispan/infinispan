package org.infinispan.client.hotrod.impl.transport.tcp;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * // TODO: Document this
 *
 * todo - this can be called from several threads, synchronize!
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RoundRobinBalancingStrategy implements RequestBalancingStrategy {

   private InetSocketAddress[] servers;
   private AtomicInteger index = new AtomicInteger();

   @Override
   public void setServers(Collection<InetSocketAddress> servers) {
      this.servers = servers.toArray(new InetSocketAddress[servers.size()]);
   }

   @Override
   public InetSocketAddress nextServer() {
      int pos = index.incrementAndGet() % servers.length;
      return servers[pos];
   }
}
