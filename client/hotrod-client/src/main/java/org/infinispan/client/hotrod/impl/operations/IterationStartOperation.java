package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class IterationStartOperation extends RetryOnFailureOperation<IterationStartResponse> {

   private final String filterConverterFactory;
   private final byte[][] filterParameters;
   private final Set<Integer> segments;
   private final int batchSize;
   private final TransportFactory transportFactory;
   private final boolean metadata;

   IterationStartOperation(Codec codec, int flags, Configuration cfg, byte[] cacheName, AtomicInteger topologyId,
                           String filterConverterFactory, byte[][] filterParameters, Set<Integer> segments,
                           int batchSize, TransportFactory transportFactory, boolean metadata) {
      super(codec, transportFactory, cacheName, topologyId, flags, cfg);
      this.filterConverterFactory = filterConverterFactory;
      this.filterParameters = filterParameters;
      this.segments = segments;
      this.batchSize = batchSize;
      this.transportFactory = transportFactory;
      this.metadata = metadata;
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(failedServers, cacheName);
   }

   @Override
   protected IterationStartResponse executeOperation(Transport transport) {
      HeaderParams params = writeHeader(transport, ITERATION_START_REQUEST);

      codec.writeIteratorStartOperation(transport, segments, filterConverterFactory, batchSize, metadata, filterParameters);

      transport.flush();

      readHeaderAndValidate(transport, params);

      return new IterationStartResponse(transport.readString(), (SegmentConsistentHash) transportFactory.getConsistentHash(cacheName), topologyId.get(), transport);
   }

   @Override
   protected void releaseTransport(Transport transport) {
   }
}
