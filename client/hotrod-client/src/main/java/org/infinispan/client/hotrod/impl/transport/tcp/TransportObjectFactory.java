package org.infinispan.client.hotrod.impl.transport.tcp;

import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.infinispan.client.hotrod.impl.protocol.HotRodOperationsHelper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TransportObjectFactory extends BaseKeyedPoolableObjectFactory {

   private static final Log log = LogFactory.getLog(TransportObjectFactory.class);
   private final TcpTransportFactory tcpTransportFactory;
   private final AtomicInteger topologyId;

   public TransportObjectFactory(TcpTransportFactory tcpTransportFactory, AtomicInteger topologyId) {
      this.tcpTransportFactory = tcpTransportFactory;
      this.topologyId = topologyId;
   }

   @Override
   public Object makeObject(Object key) throws Exception {
      InetSocketAddress serverAddress = (InetSocketAddress) key;
      TcpTransport tcpTransport = new TcpTransport(serverAddress, tcpTransportFactory);
      if (log.isTraceEnabled()) {
         log.trace("Created tcp transport: " + tcpTransport);
      }
      return tcpTransport;
   }

   /**
    * This will be called by the test thread when testWhileIdle==true.
    */
   @Override
   public boolean validateObject(Object key, Object obj) {
      TcpTransport transport = (TcpTransport) obj;
      if (log.isTraceEnabled()) {
         log.trace("About to validate(ping) connection to server " + key + ". TcpTransport is " + transport);
      }
      return HotRodOperationsHelper.ping(transport, topologyId);
   }

   @Override
   public void destroyObject(Object key, Object obj) throws Exception {
      if (log.isTraceEnabled()) {
         log.trace("About to destroy tcp transport: "+ obj);
      }
      TcpTransport transport = (TcpTransport) obj;
      transport.destroy();
   }

   @Override
   public void activateObject(Object key, Object obj) throws Exception {
      super.activateObject(key, obj);
      if (log.isTraceEnabled()) {
         log.trace("Fetching from pool:" + obj);
      }
   }

   @Override
   public void passivateObject(Object key, Object obj) throws Exception {
      super.passivateObject(key, obj);
      if (log.isTraceEnabled()) {
         log.trace("Returning to pool:" + obj);
      }
   }
}
