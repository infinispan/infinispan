package org.infinispan.client.hotrod.impl.transport.tcp;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import net.jcip.annotations.ThreadSafe;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * Round-robin implementation for {@link org.infinispan.client.hotrod.impl.transport.tcp.RequestBalancingStrategy}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RoundRobinBalancingStrategy implements RequestBalancingStrategy {

   private static final Log log = LogFactory.getLog(RoundRobinBalancingStrategy.class);

   private int index = 0;

   private SocketAddress[] servers;

   @Override
   public void setServers(Collection<SocketAddress> servers) {
      this.servers = servers.toArray(new InetSocketAddress[servers.size()]);
      // keep the old index if possible so that we don't produce more requests for the first server
      if (index >= this.servers.length) {
         index = 0;
      }
      if (log.isTraceEnabled()) {
         log.tracef("New server list is: " + Arrays.toString(this.servers));
      }
   }

   /**
    * @param failedServers Servers that should not be returned (if any other are available)
    */
   @Override
   public SocketAddress nextServer(Set<SocketAddress> failedServers) {
      for (int i = 0;; ++i) {
         SocketAddress server = getServerByIndex(index++);
         // don't allow index to overflow and have a negative value
         if (index >= servers.length)
            index = 0;

         if (failedServers == null || !failedServers.contains(server) || i >= failedServers.size()) {
            return server;
         }
      }
   }

   /**
    * Returns same value as {@link RequestBalancingStrategy#nextServer(java.util.Set} without modifying indexes/state.
    */
   public SocketAddress dryRunNextServer() {
      return getServerByIndex(index);
   }

   private SocketAddress getServerByIndex(int pos) {
      SocketAddress server = servers[pos];
      if (log.isTraceEnabled()) {
         log.tracef("Returning server: %s", server);
      }
      return server;
   }

   public SocketAddress[] getServers() {
      return servers;
   }

   public int getNextPosition() {
      return index;
   }
}
