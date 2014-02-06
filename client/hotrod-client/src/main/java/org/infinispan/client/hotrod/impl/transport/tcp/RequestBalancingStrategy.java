package org.infinispan.client.hotrod.impl.transport.tcp;

import net.jcip.annotations.ThreadSafe;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Set;

/**
 * Defines how request are distributed between the servers for replicated caches.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface RequestBalancingStrategy {

   void setServers(Collection<SocketAddress> servers);

   SocketAddress nextServer(Set<SocketAddress> failedServers);

}
