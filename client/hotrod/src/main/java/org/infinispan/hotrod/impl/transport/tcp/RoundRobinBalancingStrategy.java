package org.infinispan.hotrod.impl.transport.tcp;

import java.net.SocketAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.infinispan.hotrod.configuration.FailoverRequestBalancingStrategy;
import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;

/**
 * Round-robin implementation for {@link FailoverRequestBalancingStrategy}.
 *
 * @since 14.0
 */
public class RoundRobinBalancingStrategy implements FailoverRequestBalancingStrategy {

   private static final Log log = LogFactory.getLog(RoundRobinBalancingStrategy.class);

   private int index;

   private SocketAddress[] servers;

   @Override
   public void setServers(Collection<SocketAddress> servers) {
      this.servers = servers.toArray(new SocketAddress[servers.size()]);
      // Always start with a random server after a topology update
      index = ThreadLocalRandom.current().nextInt(this.servers.length);
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
            if (log.isTraceEnabled()) {
               if (failedServers == null)
                  log.tracef("Found server %s", server);
               else
                  log.tracef("Found server %s, with failed servers %s", server, failedServers.toString());
            }

            return server;
         }
      }
   }

   private SocketAddress getServerByIndex(int pos) {
      return servers[pos];
   }

   public SocketAddress[] getServers() {
      return servers;
   }
}
