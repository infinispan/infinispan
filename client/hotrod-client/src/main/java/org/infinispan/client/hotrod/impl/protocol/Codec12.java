package org.infinispan.client.hotrod.impl.protocol;

import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.impl.operations.BulkGetKeysOperation;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.transport.Transport;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;

/**
 * A Hot Rod encoder/decoder for version 1.2 of the protocol.
 *
 * @author Tristan Tarrant
 * @author Galder Zamarreño
 * @since 5.2
 */
public class Codec12 extends Codec11 {

   private static final Log log = LogFactory.getLog(Codec12.class, Log.class);

   @Override
   public HeaderParams writeHeader(Transport transport, HeaderParams params) {
      return writeHeader(transport, params, HotRodConstants.VERSION_12);
   }

   @Override
   protected HeaderParams writeHeader(Transport transport, HeaderParams params, byte version) {
      transport.writeByte(HotRodConstants.REQUEST_MAGIC);
      transport.writeVLong(params.messageId(MSG_ID.incrementAndGet()).messageId);
      transport.writeByte(version);
      transport.writeByte(params.opCode);
      transport.writeArray(params.cacheName);

      int joinedFlags = params.flags;
      transport.writeVInt(joinedFlags);
      transport.writeByte(params.clientIntel);
      transport.writeVInt(params.topologyId.get());
      //todo change once TX support is added
      transport.writeByte(params.txMarker);
      getLog().tracef("Wrote header for message %d. Operation code: %#04x. Flags: %#x",
            params.messageId, params.opCode, joinedFlags);
      return params;
   }

   @Override
   public Log getLog() {
      return log;
   }

   @Override
   public <K> CloseableIterator<K> keyIterator(RemoteCache<K, ?> remoteCache, OperationsFactory operationsFactory, int batchSize) {
      BulkGetKeysOperation<K> op = operationsFactory.newBulkGetKeysOperation(0);
      Set<K> keys = op.execute();
      return Closeables.iterator(keys.iterator());
   }
}
