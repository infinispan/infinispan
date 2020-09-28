package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;

import io.netty.buffer.ByteBuf;

/**
 * @since 12.0
 */
public class Codec31 extends Codec30 {
   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_31);
   }

   @Override
   public void writeBloomFilter(ByteBuf buf, int bloomFilterBits) {
      ByteBufUtil.writeVInt(buf, bloomFilterBits);
   }
}
