package org.infinispan.client.hotrod.impl.transport;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Transport factory for building and managing {@link org.infinispan.client.hotrod.impl.transport.Transport} objects.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface TransportFactory {

   public static final String CONF_HOTROD_SERVERS = "hotrod-servers";

   public Transport getTransport();

   public void releaseTransport(Transport transport);

   void start(Properties props, Collection<InetSocketAddress> staticConfiguredServers, AtomicInteger topologyId);

   void updateServers(Collection<InetSocketAddress> newServers);

   void destroy();

   void updateHashFunction(LinkedHashMap<InetSocketAddress,Integer> servers2HashCode, int numKeyOwners, short hashFunctionVersion, int hashSpace);

   Transport getTransport(byte[] key);

   boolean isTcpNoDelay();

   int getTransportCount();
}
