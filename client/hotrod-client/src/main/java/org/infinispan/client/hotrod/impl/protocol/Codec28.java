package org.infinispan.client.hotrod.impl.protocol;

import java.util.Map;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.dataconversion.MediaTypeIds;

import io.netty.buffer.ByteBuf;

/**
 * @since 9.3
 */
public class Codec28 extends Codec27 {

   @Override
   public HeaderParams writeHeader(ByteBuf buf, HeaderParams params) {
      return this.writeHeader(buf, params, HotRodConstants.VERSION_28);
   }

   @Override
   protected HeaderParams writeHeader(ByteBuf buf, HeaderParams params, byte version) {
      HeaderParams headerParams = super.writeHeader(buf, params, version);
      writeDataTypes(buf, params.dataFormat);
      return headerParams;
   }

   protected void writeDataTypes(ByteBuf buf, DataFormat dataFormat) {
      MediaType keyType = null, valueType = null;
      if (dataFormat != null) {
         keyType = dataFormat.getKeyType();
         valueType = dataFormat.getValueType();
      }
      writeMediaType(buf, keyType);
      writeMediaType(buf, valueType);
   }

   private void writeMediaType(ByteBuf buf, MediaType mediaType) {
      if (mediaType == null) {
         buf.writeByte(0);
      } else {
         Short id = MediaTypeIds.getId(mediaType);
         if (id != null) {
            buf.writeByte(1);
            ByteBufUtil.writeVInt(buf, id);
         } else {
            buf.writeByte(2);
            ByteBufUtil.writeString(buf, mediaType.toString());
         }
         Map<String, String> parameters = mediaType.getParameters();
         ByteBufUtil.writeVInt(buf, parameters.size());
         parameters.forEach((key, value) -> {
            ByteBufUtil.writeString(buf, key);
            ByteBufUtil.writeString(buf, value);
         });
      }
   }

   @Override
   public boolean allowOperationsAndEvents() {
      return true;
   }
}
