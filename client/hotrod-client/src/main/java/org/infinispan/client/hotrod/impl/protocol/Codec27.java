package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IteratorMapper;

import io.netty.buffer.ByteBuf;

/**
 * @since 9.2
 */
public class Codec27 extends Codec26 {

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_27);
   }

   @Override
   public <K> CloseableIterator<K> keyIterator(RemoteCache<K, ?> remoteCache, OperationsFactory operationsFactory,
         IntSet segments, int batchSize) {
      return new IteratorMapper<>(remoteCache.retrieveEntries(
            // Use the ToEmptyBytesKeyValueFilterConverter to remove value payload
            "org.infinispan.server.hotrod.HotRodServer$ToEmptyBytesKeyValueFilterConverter", segments, batchSize),
            e -> (K) e.getKey());
   }
}
