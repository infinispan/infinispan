package org.infinispan.client.hotrod.impl.transport.netty;

import org.infinispan.client.hotrod.impl.Transport;
import org.infinispan.client.hotrod.impl.transport.AbstractTransportFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.Properties;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class NettyTransportFactory extends AbstractTransportFactory {

   private static Log log = LogFactory.getLog(NettyTransportFactory.class);

   private InetSocketAddress serverAddr;

   @Override
   public void init(Properties props) {
      super.init(props);
      serverAddr = super.serverAddresses.iterator().next();
   }

   @Override
   public Transport getTransport() {
      log.info("Connecting to server on: " + serverAddr);
      return new NettyTransport(serverAddr);
   }

   @Override
   public void destroy() {
      // TODO: Customise this generated block
   }

   @Override
   public void releaseTransport(Transport transport) {
      transport.release();
   }
}
