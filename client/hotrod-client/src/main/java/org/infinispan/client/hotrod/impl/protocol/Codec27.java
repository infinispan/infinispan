package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorMapper;

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
         int batchSize) {
      return new CloseableIteratorMapper<>(remoteCache.retrieveEntries(
            // Use the ToEmptyBytesKeyValueFilterConverter to remove value payload
            "org.infinispan.server.hotrod.HotRodServer$ToEmptyBytesKeyValueFilterConverter", batchSize),
            e -> (K) e.getKey());
   }
}
