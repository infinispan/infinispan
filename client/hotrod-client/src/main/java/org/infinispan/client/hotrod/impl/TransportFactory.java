package org.infinispan.client.hotrod.impl;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface TransportFactory {

   public static final String CONF_HOTROD_SERVERS = "hotrod-servers";

   public Transport getTransport();

   public void releaseTransport(Transport transport);

   void start(Properties props, Collection<InetSocketAddress> staticConfiguredServers);

   void updateServers(Collection<InetSocketAddress> newServers);

   void destroy();
}
