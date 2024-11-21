package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;

import io.netty.buffer.ByteBuf;

/**
 * @since 12.0
 */
public class Codec31 extends Codec30 {
   @Override
   public void writeHeader(ByteBuf buf, long messageId, ClientTopology clientTopology, HotRodOperation<?> operation) {
      writeHeader(buf, messageId, clientTopology, operation, HotRodConstants.VERSION_31);
   }

   @Override
   public void writeBloomFilter(ByteBuf buf, int bloomFilterBits) {
      ByteBufUtil.writeVInt(buf, bloomFilterBits);
   }
}
