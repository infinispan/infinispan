package org.infinispan.client.hotrod.impl.transport.tcp;

import org.infinispan.client.hotrod.impl.Transport;
import org.infinispan.client.hotrod.impl.TransportFactory;
import org.infinispan.client.hotrod.impl.transport.AbstractTransportFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Properties;
import java.util.StringTokenizer;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TcpTransportFactory extends AbstractTransportFactory {

   private static Log log = LogFactory.getLog(TcpTransportFactory.class);

   @Override
   public void destroy() {
      // TODO: Customise this generated block
   }

   @Override
   public Transport getTransport() {
      log.info("Connecting to server on: " + serverHost + ":" + serverPort);
      TcpTransport transport = new TcpTransport(serverHost, serverPort);
      transport.connect();
      return transport;
   }
}
