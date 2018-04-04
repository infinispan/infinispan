package org.infinispan.client.hotrod.impl.protocol;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Set;

import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;

import io.netty.buffer.ByteBuf;

public class Codec24 extends Codec23 {

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_24);
   }

   @Override
   public void writeIteratorStartOperation(ByteBuf buf, Set<Integer> segments, String filterConverterFactory, int batchSize, boolean metadata, byte[][] filterParameters) {
      if (segments == null) {
         ByteBufUtil.writeSignedVInt(buf, -1);
      } else {
         // TODO use a more compact BitSet implementation, like http://roaringbitmap.org/
         BitSet bitSet = new BitSet();
         segments.stream().forEach(bitSet::set);
         ByteBufUtil.writeOptionalArray(buf, bitSet.toByteArray());
      }
      ByteBufUtil.writeOptionalString(buf, filterConverterFactory);
      if (filterConverterFactory != null) {
         if (filterParameters != null && filterParameters.length > 0) {
            buf.writeByte(filterParameters.length);
            Arrays.stream(filterParameters).forEach(param -> ByteBufUtil.writeArray(buf, param));
         } else {
            buf.writeByte(0);
         }
      }
      ByteBufUtil.writeVInt(buf, batchSize);
      buf.writeByte(metadata ? 1 : 0);
   }

   @Override
   public int readProjectionSize(ByteBuf buf) {
      return ByteBufUtil.readVInt(buf);
   }
}
