package org.infinispan.client.hotrod.impl.protocol;

import io.netty.buffer.ByteBuf;

/**
 * @author gustavonalle
 * @since 8.2
 */
public class Codec25 extends Codec24 {

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_25);
   }
}
