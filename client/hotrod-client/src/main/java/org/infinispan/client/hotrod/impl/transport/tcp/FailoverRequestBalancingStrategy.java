package org.infinispan.client.hotrod.impl.transport.tcp;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Set;

public interface FailoverRequestBalancingStrategy {

   void setServers(Collection<SocketAddress> servers);

   SocketAddress nextServer(Set<SocketAddress> failedServers);

}
