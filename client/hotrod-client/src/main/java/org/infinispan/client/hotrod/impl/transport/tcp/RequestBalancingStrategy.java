package org.infinispan.client.hotrod.impl.transport.tcp;

import net.jcip.annotations.ThreadSafe;

import java.net.InetSocketAddress;
import java.util.Collection;

/**
 * Defines how request are distributed between the servers for replicated caches. This class must be thread safe: setServer
 * and nextServer are called from multiple threads. 
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@ThreadSafe
public interface RequestBalancingStrategy {

   void setServers(Collection<InetSocketAddress> servers);

   InetSocketAddress nextServer();
}
