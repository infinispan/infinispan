package org.infinispan.client.hotrod.impl.operations;

import java.net.SocketAddress;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.consistenthash.ConsistentHash;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class IterationStartOperation extends RetryOnFailureOperation<IterationStartResponse> {

   private static final Log log = LogFactory.getLog(IterationStartOperation.class);


   private final String filterConverterFactory;
   private final Set<Integer> segments;
   private final int batchSize;
   private final TransportFactory transportFactory;

   protected IterationStartOperation(Codec codec, Flag[] flags, byte[] cacheName, AtomicInteger topologyId,
                                     String filterConverterFactory, Set<Integer> segments, int batchSize, TransportFactory transportFactory) {
      super(codec, transportFactory, cacheName, topologyId, flags);
      this.filterConverterFactory = filterConverterFactory;
      this.segments = segments;
      this.batchSize = batchSize;
      this.transportFactory = transportFactory;
   }

   @Override
   protected Transport getTransport(int retryCount, Set<SocketAddress> failedServers) {
      return transportFactory.getTransport(failedServers, cacheName);
   }

   @Override
   protected IterationStartResponse executeOperation(Transport transport) {
      HeaderParams params = writeHeader(transport, ITERATION_START_REQUEST);
      if (segments == null) {
         transport.writeSignedVInt(-1);
      } else {
         // TODO use a more compact BitSet implementation, like http://roaringbitmap.org/
         BitSet bitSet = new BitSet();
         segments.stream().forEach(bitSet::set);
         transport.writeOptionalArray(bitSet.toByteArray());
      }
      transport.writeOptionalString(filterConverterFactory);
      transport.writeVInt(batchSize);
      transport.flush();

      readHeaderAndValidate(transport, params);

      return new IterationStartResponse(transport.readString(), (SegmentConsistentHash) transportFactory.getConsistentHash(cacheName), topologyId.get(), transport);
   }

   @Override
   protected void releaseTransport(Transport transport) {
   }
}
