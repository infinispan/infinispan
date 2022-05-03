package org.infinispan.hotrod.impl.protocol;

import io.netty.buffer.ByteBuf;

/**
 * @since 14.0
 */
public class Codec25 extends Codec24 {

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_25);
   }

   @Override
   public short readMeta(ByteBuf buf) {
      return buf.readUnsignedByte();
   }
}
