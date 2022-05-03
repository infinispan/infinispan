package org.infinispan.hotrod.impl.protocol;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.hotrod.impl.cache.RemoteCache;
import org.infinispan.hotrod.impl.operations.CacheOperationsFactory;

import io.netty.buffer.ByteBuf;

/**
 * @since 14.0
 */
public class Codec27 extends Codec26 {

   public static final String EMPTY_VALUE_CONVERTER = "org.infinispan.server.hotrod.HotRodServer$ToEmptyBytesKeyValueFilterConverter";

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_27);
   }

   @Override
   public <K> CloseableIterator<K> keyIterator(RemoteCache<K, ?> remoteCache, CacheOperationsFactory cacheOperationsFactory, CacheOptions options, IntSet segments, int batchSize) {
      return new IteratorMapper<>(remoteCache.retrieveEntries(
            // Use the ToEmptyBytesKeyValueFilterConverter to remove value payload
            EMPTY_VALUE_CONVERTER, segments, batchSize), e -> (K) e.key());
   }
}
