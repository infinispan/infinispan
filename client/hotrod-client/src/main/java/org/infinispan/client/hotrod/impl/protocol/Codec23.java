package org.infinispan.client.hotrod.impl.protocol;

import java.util.BitSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.CloseableIteratorMapper;

import io.netty.buffer.ByteBuf;

/**
 * @author gustavonalle
 * @since 8.0
 */
public class Codec23 extends Codec22 {

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_23);
   }

   @Override
   public <K> CloseableIterator<K> keyIterator(RemoteCache<K, ?> remoteCache, OperationsFactory operationsFactory, int batchSize) {
      // Retrieve key and entry but map to key
      return new CloseableIteratorMapper<>(remoteCache.retrieveEntries(null, batchSize), e -> (K) e.getKey());
   }

   @Override
   public <K, V> CloseableIterator<Map.Entry<K, V>> entryIterator(RemoteCache<K, V> remoteCache, int batchSize) {
      return castEntryIterator(remoteCache.retrieveEntries(null, batchSize));
   }

   protected <K, V> CloseableIterator<Map.Entry<K, V>> castEntryIterator(CloseableIterator iterator) {
      return iterator;
   }

   @Override
   public void writeIteratorStartOperation(ByteBuf buf, Set<Integer> segments, String filterConverterFactory,
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
         segments.stream().forEach(bitSet::set);
         ByteBufUtil.writeOptionalArray(buf, bitSet.toByteArray());
      }
      ByteBufUtil.writeOptionalString(buf, filterConverterFactory);
      ByteBufUtil.writeVInt(buf, batchSize);
   }
}
