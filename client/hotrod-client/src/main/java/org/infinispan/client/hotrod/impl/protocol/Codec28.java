package org.infinispan.client.hotrod.impl.protocol;

import io.netty.buffer.ByteBuf;

/**
 * @since 9.3
 */
public class Codec28 extends Codec27 {
   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_28);
   }

   @Override
   public boolean allowOperationsAndEvents() {
      return true;
   }
}
