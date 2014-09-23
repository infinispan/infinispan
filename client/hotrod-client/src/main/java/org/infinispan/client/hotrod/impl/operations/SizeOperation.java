package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class SizeOperation extends RetryOnFailureOperation<Integer> {

   protected SizeOperation(Codec codec, TransportFactory transportFactory,
         byte[] cacheName, AtomicInteger topologyId, Flag[] flags) {
      super(codec, transportFactory, cacheName, topologyId, flags);
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(failedServers, cacheName);
   }

   @Override
   protected Integer executeOperation(Transport transport) {
      HeaderParams params = writeHeader(transport, SIZE_REQUEST);
      transport.flush();
      readHeaderAndValidate(transport, params);
      return transport.readVInt();
   }

}
