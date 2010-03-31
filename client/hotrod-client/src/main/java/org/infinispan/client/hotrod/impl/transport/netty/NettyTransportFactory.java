package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.client.hotrod.impl.Transport;
import org.infinispan.client.hotrod.impl.transport.AbstractTransportFactory;

import java.net.InetSocketAddress;
import java.util.Properties;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class NettyTransportFactory extends AbstractTransportFactory {
   private InetSocketAddress serverAddr;

   @Override
   public void init(Properties props) {
      super.init(props);
      serverAddr = new InetSocketAddress(serverHost, serverPort);
   }

   @Override
   public Transport getTransport() {
      return new NettyTransport(serverAddr);
   }

   @Override
   public void destroy() {
      // TODO: Customise this generated block
   }
}
