package org.infinispan.client.hotrod.impl.transport.tcp;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool.BaseKeyedPoolableObjectFactory;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.operations.PingOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TransportObjectFactory
      extends BaseKeyedPoolableObjectFactory<SocketAddress, TcpTransport> {

   private static final Log log = LogFactory.getLog(TransportObjectFactory.class);
   private static final boolean trace = log.isTraceEnabled();
   protected final TcpTransportFactory tcpTransportFactory;
   protected final AtomicInteger defaultCacheTopologyId;
   protected volatile boolean firstPingExecuted = false;
   protected final Codec codec;
   protected final Configuration configuration;

   public TransportObjectFactory(Codec codec, TcpTransportFactory tcpTransportFactory,
         AtomicInteger defaultCacheTopologyId, Configuration configuration) {
      this.tcpTransportFactory = tcpTransportFactory;
      this.defaultCacheTopologyId = defaultCacheTopologyId;
      this.codec = codec;
      this.configuration = configuration;
   }

   @Override
   public TcpTransport makeObject(SocketAddress address) throws Exception {
      TcpTransport tcpTransport = new TcpTransport(address, tcpTransportFactory);
      if (trace) log.tracef("Created tcp transport: %s", tcpTransport);
      if (!firstPingExecuted) {
         if (trace) log.trace("Executing first ping!");
         firstPingExecuted = true;

         // Don't ignore exceptions from ping() command, since
         // they indicate that the transport instance is invalid.
         ping(tcpTransport, defaultCacheTopologyId);
      }
      return tcpTransport;
   }

   protected PingOperation.PingResult ping(TcpTransport tcpTransport, AtomicInteger topologyId) {
      PingOperation po = new PingOperation(codec, topologyId, configuration, tcpTransport);
      return po.execute();
   }

   /**
    * This will be called by the test thread when testWhileIdle==true.
    */
   @Override
   public boolean validateObject(SocketAddress address, TcpTransport transport) {
      try {
         boolean valid = ping(transport, defaultCacheTopologyId).isSuccess();
         if (trace) log.tracef("Is connection %s valid? %s", transport, valid);
         return valid;
      } catch (Throwable e) {
         if (trace) log.tracef(e, "Error validating the connection %s. Marking it as invalid.", transport);
         return false;
      }
   }

   @Override
   public void destroyObject(SocketAddress address, TcpTransport transport) throws Exception {
      if (trace) log.tracef("About to destroy tcp transport: %s", transport);
      transport.release();
   }

   @Override
   public void activateObject(SocketAddress address, TcpTransport transport) throws Exception {
      super.activateObject(address, transport);
      if (trace) log.tracef("Fetching from pool: %s", transport);
   }

   @Override
   public void passivateObject(SocketAddress address, TcpTransport transport) throws Exception {
      super.passivateObject(address, transport);
      if (trace) log.tracef("Returning to pool: %s", transport);
   }
}
