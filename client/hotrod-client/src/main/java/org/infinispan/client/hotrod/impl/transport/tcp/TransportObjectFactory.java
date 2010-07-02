package org.infinispan.client.hotrod.impl.transport.tcp;

import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.protocol.HotRodOperationsHelper;
import org.infinispan.manager.DefaultCacheManager;
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
   private static final byte[] DEFAULT_CACHE_NAME_BYTES = new byte[]{};

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
      try {
         if (log.isTraceEnabled()) {
            log.trace("About to validate(ping) connection to server " + key + ". TcpTransport is " + transport);
         }
         long messageId = HotRodOperationsHelper.writeHeader(transport, HotRodConstants.PING_REQUEST, DEFAULT_CACHE_NAME_BYTES, topologyId);
         short respStatus = HotRodOperationsHelper.readHeaderAndValidate(transport, messageId, HotRodConstants.PING_RESPONSE, topologyId);
         if (respStatus == HotRodConstants.NO_ERROR_STATUS) {
            if (log.isTraceEnabled())
               log.trace("Successfully validated transport: " + transport);
            return true;
         } else {
            if (log.isTraceEnabled())
               log.trace("Unknown response status: " + respStatus);
            return false;
         }
      } catch (Exception e) {
         if (log.isTraceEnabled())
            log.trace("Failed to validate transport: " + transport, e);
         return false;
      }
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
