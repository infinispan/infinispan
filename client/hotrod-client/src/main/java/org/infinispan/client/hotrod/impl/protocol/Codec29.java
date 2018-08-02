package org.infinispan.client.hotrod.impl.protocol;

import io.netty.buffer.ByteBuf;

/**
 * @since 9.4
 */
public class Codec29 extends Codec28 {

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      HeaderParams headerParams = writeHeader(buf, params, HotRodConstants.VERSION_29);
      writeDataTypes(buf, params.dataFormat);
      return headerParams;
   }


}
