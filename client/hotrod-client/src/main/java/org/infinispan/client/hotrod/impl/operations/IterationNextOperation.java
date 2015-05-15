package org.infinispan.client.hotrod.impl.operations;

import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.transport.Transport;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class IterationNextOperation extends HotRodOperation {
   private final String iterationId;
   private final Transport transport;

   protected IterationNextOperation(Codec codec, Flag[] flags, byte[] cacheName, AtomicInteger topologyId,
                                    String iterationId, Transport transport) {
      super(codec, flags, cacheName, topologyId);
      this.iterationId = iterationId;
      this.transport = transport;

   }

   @Override
   public IterationNextResponse execute() {
      HeaderParams params = writeHeader(transport, ITERATION_NEXT_REQUEST);

      transport.writeString(iterationId);
      transport.flush();

      short status = readHeaderAndValidate(transport, params);
      byte[] finishedSegments = transport.readArray();
      int entriesSize = transport.readVInt();
      Map.Entry<byte[], byte[]>[] entries = new Map.Entry[entriesSize];
      for (int i = 0; i < entriesSize; i++) {
         byte[] key = transport.readArray();
         byte[] value = transport.readArray();
         entries[i] = new SimpleEntry<>(key, value);
      }

      return new IterationNextResponse(status, finishedSegments, entries);

   }
}
