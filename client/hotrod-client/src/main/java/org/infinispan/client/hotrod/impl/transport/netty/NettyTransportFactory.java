package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.client.hotrod.impl.Transport;
import org.infinispan.client.hotrod.impl.TransportFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Properties;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class NettyTransportFactory implements TransportFactory {

   private static Log log = LogFactory.getLog(NettyTransportFactory.class);

   private InetSocketAddress serverAddr;
   private Collection<InetSocketAddress> serverAddresses;

   @Override
   public void start(Properties props, Collection<InetSocketAddress> staticConfiguredServers) {
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
      return new NettyTransport(serverAddr);
   }

   @Override
   public void destroy() {
      //nothing to do here as this no pooling is available
   }

   @Override
   public void releaseTransport(Transport transport) {
      transport.release();
   }
}
