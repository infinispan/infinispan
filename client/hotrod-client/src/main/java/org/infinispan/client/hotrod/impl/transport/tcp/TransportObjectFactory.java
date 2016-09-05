package org.infinispan.client.hotrod.impl.transport.tcp;

import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.pool2.KeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.infinispan.client.hotrod.impl.operations.PingOperation;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class TransportObjectFactory implements KeyedPooledObjectFactory<SocketAddress, TcpTransport> {

   private static final Log log = LogFactory.getLog(TransportObjectFactory.class);
   private static final boolean trace = log.isTraceEnabled();
   protected final TcpTransportFactory tcpTransportFactory;
   protected final AtomicInteger defaultCacheTopologyId;
   protected volatile boolean firstPingExecuted = false;
   protected final Codec codec;

   public TransportObjectFactory(Codec codec, TcpTransportFactory tcpTransportFactory,
         AtomicInteger defaultCacheTopologyId) {
      this.tcpTransportFactory = tcpTransportFactory;
      this.defaultCacheTopologyId = defaultCacheTopologyId;
      this.codec = codec;
   }

   @Override
   public PooledObject<TcpTransport> makeObject(SocketAddress address) throws Exception {
      TcpTransport tcpTransport = new TcpTransport(address, tcpTransportFactory);
      if (trace) log.tracef("Created tcp transport: %s", tcpTransport);
      if (!firstPingExecuted) {
         if (trace) log.trace("Executing first ping!");
         firstPingExecuted = true;

         // Don't ignore exceptions from ping() command, since
         // they indicate that the transport instance is invalid.
         ping(tcpTransport, defaultCacheTopologyId);
      }
      return new DefaultPooledObject(tcpTransport);
   }

   @Override
   public void destroyObject(SocketAddress address, PooledObject<TcpTransport> transport) throws Exception {
      if (trace) log.tracef("About to destroy tcp transport: %s", transport);
      transport.getObject().release();
   }

   @Override
   public boolean validateObject(SocketAddress address, PooledObject<TcpTransport> transport) {
      try {
         boolean valid = ping(transport.getObject(), defaultCacheTopologyId).isSuccess();
         if (trace) log.tracef("Is connection %s valid? %s", transport, valid);
         return valid;
      } catch (Throwable e) {
         if (trace) log.tracef(e, "Error validating the connection %s. Marking it as invalid.", transport);
         return false;
      }
   }

   @Override
   public void activateObject(SocketAddress address, PooledObject<TcpTransport> transport) throws Exception {
      if (trace) log.tracef("Fetching from pool: %s", transport);
   }

   @Override
   public void passivateObject(SocketAddress address, PooledObject<TcpTransport> transport) throws Exception {
      if (trace) log.tracef("Returning to pool: %s", transport);
   }

   protected PingOperation.PingResult ping(TcpTransport tcpTransport, AtomicInteger topologyId) {
      PingOperation po = new PingOperation(codec, topologyId, tcpTransport);
      return po.execute();
   }

}
