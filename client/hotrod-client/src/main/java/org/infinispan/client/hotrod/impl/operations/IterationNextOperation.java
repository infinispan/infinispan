package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.MetadataValueImpl;
import org.infinispan.client.hotrod.impl.iteration.KeyTracker;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.client.hotrod.marshall.MarshallerUtil;
import org.infinispan.commons.marshall.Marshaller;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class IterationNextOperation<E> extends HotRodOperation {

   private static final Log log = LogFactory.getLog(IterationNextOperation.class);

   private final String iterationId;
   private final Transport transport;
   private final List<String> whitelist;
   private final KeyTracker segmentKeyTracker;

   protected IterationNextOperation(Codec codec, int flags, Configuration cfg, byte[] cacheName, AtomicInteger topologyId,
                                    String iterationId, Transport transport, KeyTracker segmentKeyTracker, List<String> whitelist) {
      super(codec, flags, cfg, cacheName, topologyId);
      this.iterationId = iterationId;
      this.transport = transport;
      this.whitelist = whitelist;
      this.segmentKeyTracker = segmentKeyTracker;
   }

   @Override
   public IterationNextResponse<E> execute() {
      HeaderParams params = writeHeader(transport, ITERATION_NEXT_REQUEST);

      transport.writeString(iterationId);
      transport.flush();

      short status = readHeaderAndValidate(transport, params);
      byte[] finishedSegments = transport.readArray();

      int entriesSize = transport.readVInt();
      List<Entry<Object, E>> entries = new ArrayList<>(entriesSize);
      if (entriesSize > 0) {
         int projectionsSize = transport.readVInt();
         for (int i = 0; i < entriesSize; i++) {
            short meta = transport.readByte();
            long creation = -1;
            int lifespan = -1;
            long lastUsed = -1;
            int maxIdle = -1;
            long version = 0;
            if (meta == 1) {
               short flags = transport.readByte();
               if ((flags & INFINITE_LIFESPAN) != INFINITE_LIFESPAN) {
                  creation = transport.readLong();
                  lifespan = transport.readVInt();
               }
               if ((flags & INFINITE_MAXIDLE) != INFINITE_MAXIDLE) {
                  lastUsed = transport.readLong();
                  maxIdle = transport.readVInt();
               }
               version = transport.readLong();
            }
            byte[] key = transport.readArray();
            Object value;
            if (projectionsSize > 1) {
               Object[] projections = new Object[projectionsSize];
               for (int j = 0; j < projectionsSize; j++) {
                  projections[j] = unmarshall(transport.readArray(), status);
               }
               value = projections;
            } else {
               value = unmarshall(transport.readArray(), status);
            }
            if (meta == 1) {
               value = new MetadataValueImpl<>(creation, lifespan, lastUsed, maxIdle, version, value);
            }

            if (segmentKeyTracker.track(key, status, whitelist)) {
               entries.add(new SimpleEntry<>(unmarshall(key, status), (E) value));
            }
         }
      }
      segmentKeyTracker.segmentsFinished(finishedSegments);
      if (HotRodConstants.isInvalidIteration(status)) {
         throw log.errorRetrievingNext(iterationId);
      }
      return new IterationNextResponse(status, entries, entriesSize > 0);
   }

   private Object unmarshall(byte[] bytes, short status) {
      Marshaller marshaller = transport.getTransportFactory().getMarshaller();
      return MarshallerUtil.bytes2obj(marshaller, bytes, status, whitelist);
   }

}
