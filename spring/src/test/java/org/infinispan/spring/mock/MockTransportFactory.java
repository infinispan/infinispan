package org.infinispan.spring.mock;

import java.net.SocketAddress;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import javax.net.ssl.SSLContext;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHashFactory;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

public final class MockTransportFactory implements TransportFactory {

   @Override
   public Transport getTransport(Set<SocketAddress> failedServers) {
      return null;
   }

   @Override
   public void releaseTransport(final Transport transport) {
   }

   @Override
   public void start(Codec codec, Configuration configuration, AtomicInteger topologyId) {
   }

   @Override
   public void updateServers(final Collection<SocketAddress> newServers) {
   }

   @Override
   public void destroy() {
   }

   @Override
   public void updateHashFunction(final Map<SocketAddress, Set<Integer>> servers2Hash,
            final int numKeyOwners, final short hashFunctionVersion, final int hashSpace) {
   }

   @Override
   public Transport getTransport(final byte[] key, Set<SocketAddress> failedServers) {
      return null;
   }

   @Override
   public boolean isTcpNoDelay() {
      return false;
   }

   @Override
   public int getMaxRetries() {
      return 0;
   }

   @Override
   public int getSoTimeout() {
      return 1000;
   }

   @Override
   public int getConnectTimeout() {
      return 1000;
   }

   @Override
   public void invalidateTransport(SocketAddress serverAddress, Transport transport) {
      // Do nothing

   }

   @Override
   public ConsistentHashFactory getConsistentHashFactory() {
      return null;
   }

   @Override
   public SSLContext getSSLContext() {
      return null;
   }
}
