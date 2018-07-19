package org.infinispan.client.hotrod.impl.protocol;

import static org.infinispan.client.hotrod.impl.Util.await;

import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.impl.operations.BulkGetKeysOperation;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.Closeables;
import org.infinispan.commons.util.IntSet;

import io.netty.buffer.ByteBuf;

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
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_12);
   }

   @Override
   protected HeaderParams writeHeader(ByteBuf buf, HeaderParams params, byte version) {
      buf.writeByte(HotRodConstants.REQUEST_MAGIC);
      ByteBufUtil.writeVLong(buf, params.messageId);
      buf.writeByte(version);
      buf.writeByte(params.opCode);
      ByteBufUtil.writeArray(buf, params.cacheName);

      int joinedFlags = params.flags;
      ByteBufUtil.writeVInt(buf, joinedFlags);
      buf.writeByte(params.clientIntel);
      ByteBufUtil.writeVInt(buf, params.topologyId.get());
      //todo change once TX support is added
      buf.writeByte(params.txMarker);
      getLog().tracef("Wrote header for message %d. Operation code: %#04x. Flags: %#x",
            params.messageId, params.opCode, joinedFlags);
      return params;
   }

   @Override
   public Log getLog() {
      return log;
   }

   @Override
   public <K> CloseableIterator<K> keyIterator(RemoteCache<K, ?> remoteCache, OperationsFactory operationsFactory,
         IntSet segments, int batchSize) {
      if (segments != null) {
         throw new UnsupportedOperationException("This version doesn't support iterating upon keys by segment!");
      }
      BulkGetKeysOperation<K> op = operationsFactory.newBulkGetKeysOperation(0);
      Set<K> keys = await(op.execute());
      return Closeables.iterator(keys.iterator());
   }
}
