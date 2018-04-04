package org.infinispan.client.hotrod.impl.protocol;

import java.util.Arrays;
import java.util.BitSet;
import java.util.Set;

import org.infinispan.client.hotrod.impl.transport.Transport;

public class Codec24 extends Codec23 {

   @Override
   public HeaderParams writeHeader(Transport transport, HeaderParams params) {
      return writeHeader(transport, params, HotRodConstants.VERSION_24);
   }

   @Override
   public void writeIteratorStartOperation(Transport transport, Set<Integer> segments, String filterConverterFactory,
         int batchSize, boolean metadata, byte[][] filterParameters) {
      if (segments == null) {
         transport.writeSignedVInt(-1);
      } else {
         // TODO use a more compact BitSet implementation, like http://roaringbitmap.org/
         BitSet bitSet = new BitSet();
         segments.stream().forEach(bitSet::set);
         transport.writeOptionalArray(bitSet.toByteArray());
      }
      transport.writeOptionalString(filterConverterFactory);
      if (filterConverterFactory != null) {
         if (filterParameters != null && filterParameters.length > 0) {
            transport.writeByte((short) filterParameters.length);
            Arrays.stream(filterParameters).forEach(transport::writeArray);
         } else {
            transport.writeByte((short) 0);
         }
      }
      transport.writeVInt(batchSize);
      transport.writeByte((short) (metadata ? 1 : 0));
   }

   @Override
   public int readProjectionSize(Transport transport) {
      return transport.readVInt();
   }
}
