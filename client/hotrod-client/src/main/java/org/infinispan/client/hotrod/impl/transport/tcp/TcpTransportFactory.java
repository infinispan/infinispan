package org.infinispan.client.hotrod.impl.transport.tcp;

import org.infinispan.client.hotrod.impl.Transport;
import org.infinispan.client.hotrod.impl.TransportFactory;
import org.infinispan.client.hotrod.impl.transport.AbstractTransportFactory;
import org.infinispan.client.hotrod.impl.transport.VHelper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
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

   private TcpConnectionPool connectionPool;

   @Override
   public void init(Properties props) {
      super.init(props);
      String tcpConnectionPool = props.getProperty(CONF_TCP_CONNECTION_POOL);
      if (tcpConnectionPool == null) {
         tcpConnectionPool = DefaultTcpConnectionPool.class.getName();
         log.trace("No tcp connection pools specified, using the default: " + tcpConnectionPool);
      }
      connectionPool = (TcpConnectionPool) VHelper.newInstance(tcpConnectionPool);
      connectionPool.init(props);
      connectionPool.start(serverAddresses);
   }

   @Override
   public void destroy() {
      if (connectionPool != null) {
         connectionPool.destroy();
      }
   }

   @Override
   public Transport getTransport() {
      return new TcpTransport(connectionPool.getConnection());
   }

   @Override
   public void releaseTransport(Transport transport) {
      TcpTransport tcpTransport = (TcpTransport) transport;
      connectionPool.releaseConnection(tcpTransport.getSocket());
   }
}
