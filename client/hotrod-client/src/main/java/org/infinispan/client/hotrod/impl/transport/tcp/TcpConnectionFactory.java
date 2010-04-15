package org.infinispan.client.hotrod.impl.transport.tcp;

import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TcpConnectionFactory extends BaseKeyedPoolableObjectFactory {

   private static Log log = LogFactory.getLog(TcpConnectionFactory.class);

   @Override
   public Object makeObject(Object key) throws Exception {
      InetSocketAddress serverAddress = (InetSocketAddress) key;
      TcpTransport tcpTransport = new TcpTransport(serverAddress);
      if (log.isTraceEnabled()) {
         log.trace("Created tcp transport: " + tcpTransport);
      }
      return tcpTransport;
   }

   @Override
   public boolean validateObject(Object key, Object obj) {
      TcpTransport transport = (TcpTransport) obj;
      if (log.isTraceEnabled()) {
         log.trace("About to validate(ping) connection to server " + key + ". TcpTransport is " + transport);
      }
      //todo implement
      return true;
   }

   @Override
   public void destroyObject(Object key, Object obj) throws Exception {
      TcpTransport transport = (TcpTransport) obj;
      transport.destroy();
   }
}
