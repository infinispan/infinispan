package org.infinispan.client.hotrod;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Set;

/**
 * Defines what servers will be selected when a smart-routed request fails.
 */
public interface FailoverRequestBalancingStrategy {
   /**
    * Inform the strategy about the currently alive servers.
    * @param servers
    */
   void setServers(Collection<SocketAddress> servers);

   /**
    * @param failedServers
    * @return Address of the next server the request should be routed to.
    */
   SocketAddress nextServer(Set<SocketAddress> failedServers);
}
