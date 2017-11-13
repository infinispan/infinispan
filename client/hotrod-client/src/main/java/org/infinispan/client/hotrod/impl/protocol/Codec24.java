package org.infinispan.client.hotrod.impl.protocol;

import io.netty.buffer.ByteBuf;

public class Codec24 extends Codec23 {

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_24);
   }

}
