package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class IterationEndOperation extends HotRodOperation {

   private final String iterationId;
   private final TransportFactory transportFactory;
   private final Transport transport;

   protected IterationEndOperation(Codec codec, Flag[] flags, byte[] cacheName, AtomicInteger topologyId,
                                   String iterationId, TransportFactory transportFactory, Transport transport) {
      super(codec, flags, cacheName, topologyId);
      this.iterationId = iterationId;
      this.transportFactory = transportFactory;
      this.transport = transport;
   }

   @Override
   public IterationEndResponse execute() {
      try {
         HeaderParams params = writeHeader(transport, ITERATION_END_REQUEST);
         transport.writeString(iterationId);
         transport.flush();
         short status = readHeaderAndValidate(transport, params);
         return new IterationEndResponse(status);
      } finally {
         transportFactory.releaseTransport(transport);
      }
   }
}
