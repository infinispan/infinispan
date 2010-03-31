package org.infinispan.client.hotrod.impl.transport.tcp;

import org.infinispan.client.hotrod.impl.Transport;
import org.infinispan.client.hotrod.impl.TransportFactory;
import org.infinispan.client.hotrod.impl.transport.AbstractTransportFactory;

import java.util.Properties;
import java.util.StringTokenizer;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TcpTransportFactory extends AbstractTransportFactory {

   @Override
   public void destroy() {
      // TODO: Customise this generated block
   }

   @Override
   public Transport getTransport() {
      TcpTransport transport = new TcpTransport(serverHost, serverPort);
      transport.connect();
      return transport;
   }
}
