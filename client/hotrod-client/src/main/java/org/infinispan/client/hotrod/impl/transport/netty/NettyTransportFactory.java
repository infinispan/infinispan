package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class NettyTransportFactory implements TransportFactory {

   private static Log log = LogFactory.getLog(NettyTransportFactory.class);

   private InetSocketAddress serverAddr;
   private Collection<InetSocketAddress> serverAddresses;

   @Override
   public void start(Properties props, Collection<InetSocketAddress> staticConfiguredServers, AtomicInteger topologyId) {
      this.serverAddresses = staticConfiguredServers;
      serverAddr = serverAddresses.iterator().next();
   }

   @Override
   public void updateServers(Collection<InetSocketAddress> newServers) {
      throw new IllegalStateException();
   }

   @Override
   public Transport getTransport() {
      log.info("Connecting to server on: " + serverAddr);
      return new NettyTransport(serverAddr, this);
   }

   @Override
   public void destroy() {
      //nothing to do here as this no pooling is available
   }

   @Override
   public void releaseTransport(Transport transport) {
      transport.release();
   }

   @Override
   public void updateHashFunction(LinkedHashMap<InetSocketAddress,Integer> servers2HashCode, int numKeyOwners, short hashFunctionVersion, int hashSpace) {
      // TODO: Customise this generated block
   }

   @Override
   public Transport getTransport(byte[] key) {
      return getTransport();  // TODO: Customise this generated block
   }
}
