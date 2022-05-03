package org.infinispan.hotrod.impl.protocol;

import java.util.BitSet;
import java.util.function.IntConsumer;

import org.infinispan.api.common.CacheEntry;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IteratorMapper;
import org.infinispan.hotrod.impl.cache.RemoteCache;
import org.infinispan.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;

import io.netty.buffer.ByteBuf;

/**
 * @since 14.0
 */
public class Codec23 extends Codec22 {

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_23);
   }

   @Override
   public <K> CloseableIterator<K> keyIterator(RemoteCache<K, ?> remoteCache, CacheOperationsFactory cacheOperationsFactory,
                                               CacheOptions options, IntSet segments, int batchSize) {
      // Retrieve key and entry but map to key
      return new IteratorMapper<>(remoteCache.retrieveEntries(null, segments, batchSize), e -> (K) e.key());
   }

   @Override
   public <K, V> CloseableIterator<CacheEntry<K, V>> entryIterator(RemoteCache<K, V> remoteCache, IntSet segments,
                                                                   int batchSize) {
      return castEntryIterator(remoteCache.retrieveEntries(null, segments, batchSize));
   }

   protected <K, V> CloseableIterator<CacheEntry<K, V>> castEntryIterator(CloseableIterator iterator) {
      return iterator;
   }

   @Override
   public void writeIteratorStartOperation(ByteBuf buf, IntSet segments, String filterConverterFactory,
         int batchSize, boolean metadata, byte[][] filterParameters) {
      if (metadata) {
         throw new UnsupportedOperationException("Metadata for entry iteration not supported in this version!");
      }
      if (segments == null) {
         ByteBufUtil.writeSignedVInt(buf, -1);
      } else {
         if (filterParameters != null && filterParameters.length > 0) {
            throw new UnsupportedOperationException("The filterParamters for entry iteration are not supported in this version!");
         }
         // TODO use a more compact BitSet implementation, like http://roaringbitmap.org/
         BitSet bitSet = new BitSet();
         segments.forEach((IntConsumer) bitSet::set);
         ByteBufUtil.writeOptionalArray(buf, bitSet.toByteArray());
      }
      ByteBufUtil.writeOptionalString(buf, filterConverterFactory);
      ByteBufUtil.writeVInt(buf, batchSize);
   }
}
