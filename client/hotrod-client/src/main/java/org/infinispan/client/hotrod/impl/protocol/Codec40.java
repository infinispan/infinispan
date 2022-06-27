package org.infinispan.client.hotrod.impl.protocol;

import java.util.Map;

import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;

import io.netty.buffer.ByteBuf;

/**
 * @since 14.0
 */
public class Codec40 extends Codec31 {

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return writeHeader(buf, params, HotRodConstants.VERSION_40);
   }

   @Override
   protected HeaderParams writeHeader(ByteBuf buf, HeaderParams params, byte version) {
      HeaderParams headerParams = super.writeHeader(buf, params, version);
      writeOtherParams(buf, params.otherParams());
      return headerParams;
   }

   private void writeOtherParams(ByteBuf buf, Map<String, byte[]> parameters) {
      if (parameters == null) {
         ByteBufUtil.writeVInt(buf, 0);
         return;
      }

      ByteBufUtil.writeVInt(buf, parameters.size());
      parameters.forEach((key, value) -> {
         ByteBufUtil.writeString(buf, key);
         ByteBufUtil.writeArray(buf, value);
      });
   }
}
