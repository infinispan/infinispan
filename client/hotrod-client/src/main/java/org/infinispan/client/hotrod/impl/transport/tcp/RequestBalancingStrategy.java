package org.infinispan.client.hotrod.impl.transport.tcp;

import java.net.SocketAddress;
import java.util.Collection;

/**
 * Defines how request are distributed between the servers for replicated caches.
 *
 * @author Mircea.Markus@jboss.com
 * @deprecated Please extend {@link FailoverRequestBalancingStrategy} instead.
 * @since 4.1
 */
@Deprecated
public interface RequestBalancingStrategy {

   void setServers(Collection<SocketAddress> servers);

   /**
    * @deprecated This method will be removed.
    * {@link org.infinispan.client.hotrod.impl.transport.tcp.FailoverRequestBalancingStrategy#nextServer(java.util.Set)} will replace it.
    */
   @Deprecated
   SocketAddress nextServer();

}
