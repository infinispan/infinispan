package org.infinispan.client.hotrod.impl.transport.tcp;

import java.net.InetSocketAddress;
import java.util.Collection;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface RequestBalancingStrategy {

   void setServers(Collection<InetSocketAddress> servers);

   InetSocketAddress nextServer();
}
