package org.infinispan.hotrod.impl.protocol;

import io.netty.buffer.ByteBuf;

/**
 * @since 14.0
 */
public class Codec30 extends Codec29 {
   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_30);
   }
}
