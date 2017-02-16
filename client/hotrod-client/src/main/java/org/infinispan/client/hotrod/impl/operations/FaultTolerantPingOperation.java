package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.ClientIntelligence;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * A fault tolerant ping operation that can survive to node failures.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
public class FaultTolerantPingOperation extends RetryOnFailureOperation<PingOperation.PingResult> {

   protected FaultTolerantPingOperation(Codec codec, TransportFactory transportFactory,
                                        byte[] cacheName, AtomicInteger topologyId, int flags,
                                        ClientIntelligence clientIntelligence) {
      super(codec, transportFactory, cacheName, topologyId, flags, clientIntelligence);
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(failedServers, cacheName);
   }

   @Override
   protected PingOperation.PingResult executeOperation(Transport transport) {
      return new PingOperation(codec, topologyId, clientIntelligence, transport, cacheName).execute();
   }

}
