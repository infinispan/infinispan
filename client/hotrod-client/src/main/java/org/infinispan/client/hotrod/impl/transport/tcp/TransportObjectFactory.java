package org.infinispan.client.hotrod.impl.transport.tcp;

import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.infinispan.client.hotrod.impl.operations.PingOperation;
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
   private final boolean pingOnStartup;
   private volatile boolean firstPingExecuted = false;

   public TransportObjectFactory(TcpTransportFactory tcpTransportFactory, AtomicInteger topologyId, boolean pingOnStartup) {
      this.tcpTransportFactory = tcpTransportFactory;
      this.topologyId = topologyId;
      this.pingOnStartup = pingOnStartup;
   }

   @Override
   public Object makeObject(Object key) throws Exception {
      InetSocketAddress serverAddress = (InetSocketAddress) key;
      TcpTransport tcpTransport = new TcpTransport(serverAddress, tcpTransportFactory);
      if (log.isTraceEnabled()) {
         log.trace("Created tcp transport: " + tcpTransport);
      }
      if (pingOnStartup && !firstPingExecuted) {
         log.trace("Executing first ping!");
         firstPingExecuted = true;
         try {
            ping(tcpTransport, topologyId);
         } catch (Exception e) {
            log.trace("Ignoring ping request failure during ping on startup: " + e.getMessage());
         }
      }
      return tcpTransport;
   }

   private PingOperation.PingResult ping(TcpTransport tcpTransport, AtomicInteger topologyId) {
      PingOperation po = new PingOperation(topologyId, tcpTransport);
      return po.execute();
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
      return ping(transport, topologyId) == PingOperation.PingResult.SUCCESS;
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
