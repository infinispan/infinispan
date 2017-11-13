package org.infinispan.client.hotrod.impl.protocol;

import io.netty.buffer.ByteBuf;

/**
 * @since 9.2
 */
public class Codec27 extends Codec26 {

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_27);
   }
}
