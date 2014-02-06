package org.infinispan.client.hotrod.impl.transport;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.protocol.Codec;

/**
 * Transport factory for building and managing {@link org.infinispan.client.hotrod.impl.transport.Transport} objects.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public interface TransportFactory {

   Transport getTransport(Set<SocketAddress> failedServers);

   void releaseTransport(Transport transport);

   void start(Codec codec, Configuration configuration, AtomicInteger topologyId);

   void updateServers(Collection<SocketAddress> newServers);

   void destroy();

   void updateHashFunction(Map<SocketAddress, Set<Integer>> servers2Hash, int numKeyOwners, short hashFunctionVersion, int hashSpace);

   ConsistentHashFactory getConsistentHashFactory();

   Transport getTransport(byte[] key, Set<SocketAddress> failedServers);

   boolean isTcpNoDelay();

   int getMaxRetries();

   int getSoTimeout();

   int getConnectTimeout();

   void invalidateTransport(SocketAddress serverAddress, Transport transport);

   SSLContext getSSLContext();
}
