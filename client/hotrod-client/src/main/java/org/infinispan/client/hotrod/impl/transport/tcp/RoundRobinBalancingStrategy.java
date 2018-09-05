package org.infinispan.client.hotrod.impl.transport.tcp;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * Round-robin implementation for {@link org.infinispan.client.hotrod.impl.transport.tcp.FailoverRequestBalancingStrategy}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class RoundRobinBalancingStrategy implements FailoverRequestBalancingStrategy {

   private static final Log log = LogFactory.getLog(RoundRobinBalancingStrategy.class);
   private static final boolean trace = log.isTraceEnabled();

   private int index;

   private SocketAddress[] servers;

   @Override
   public void setServers(Collection<SocketAddress> servers) {
      this.servers = servers.toArray(new SocketAddress[servers.size()]);
      // Always start with a random server after a topology update
      index = ThreadLocalRandom.current().nextInt(this.servers.length);
      if (trace) {
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
            if (trace) {
               if (failedServers == null)
                  log.tracef("Selected %s from %s", server, Arrays.toString(servers));
               else
                  log.tracef("Selected %s from %s, with failed servers %s", server, Arrays.toString(servers), failedServers.toString());
            }

            return server;
         }
      }
   }

   /**
    * Returns same value as {@link FailoverRequestBalancingStrategy#nextServer(java.util.Set)} without modifying indexes/state.
    */
   public SocketAddress dryRunNextServer() {
      return getServerByIndex(index);
   }

   private SocketAddress getServerByIndex(int pos) {
      SocketAddress server = servers[pos];
      if (trace) {
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
