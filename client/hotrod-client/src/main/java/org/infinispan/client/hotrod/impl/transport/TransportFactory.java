package org.infinispan.client.hotrod.impl.transport;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import javax.net.ssl.SSLContext;

import org.infinispan.client.hotrod.CacheTopologyInfo;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.event.ClientListenerNotifier;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.tcp.TcpTransportFactory.ClusterSwitchStatus;
import org.infinispan.commons.marshall.Marshaller;

/**
 * Transport factory for building and managing {@link org.infinispan.client.hotrod.impl.transport.Transport} objects.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 * @deprecated since 9.2, implemenations not called anymore
 */
@Deprecated
public interface TransportFactory {

   Transport getTransport(Set<SocketAddress> failedServers, byte[] cacheName);

   Transport getAddressTransport(SocketAddress server);

   SocketAddress getSocketAddress(Object key, byte[] cacheName);

   void releaseTransport(Transport transport);

   void start(Codec codec, Configuration configuration, AtomicInteger topologyId,
         ClientListenerNotifier listenerNotifier, Collection<Consumer<Set<SocketAddress>>> failedServerNotifier);

   void updateServers(Collection<SocketAddress> newServers, byte[] cacheName, boolean quiet);

   void destroy();

   CacheTopologyInfo getCacheTopologyInfo(byte[] cacheName);

   /**
    * @deprecated Only called for Hot Rod 1.x protocol.
    */
   @Deprecated
   void updateHashFunction(Map<SocketAddress, Set<Integer>> servers2Hash, int numKeyOwners, short hashFunctionVersion, int hashSpace,
      byte[] cacheName, AtomicInteger topologyId);

   void updateHashFunction(SocketAddress[][] segmentOwners, int numSegments, short hashFunctionVersion,
      byte[] cacheName, AtomicInteger topologyId);

   ConsistentHash getConsistentHash(byte[] cacheName);

   ConsistentHashFactory getConsistentHashFactory();

   Transport getTransport(Object key, Set<SocketAddress> failedServers, byte[] cacheName);

   boolean isTcpNoDelay();

   boolean isTcpKeepAlive();

   int getMaxRetries();

   int getSoTimeout();

   int getConnectTimeout();

   void invalidateTransport(SocketAddress serverAddress, Transport transport);

   SSLContext getSSLContext();

   void reset(byte[] cacheName);

   AtomicInteger createTopologyId(byte[] cacheName);

   int getTopologyId(byte[] cacheName);

   ClusterSwitchStatus trySwitchCluster(String failedClusterName, byte[] cacheName);

   Marshaller getMarshaller();

   boolean switchToCluster(String clusterName);

   String getCurrentClusterName();

   int getTopologyAge();

   String getSniHostName();

   void addDisconnectedListener(Runnable reconnectionRunnable) throws InterruptedException;
}
