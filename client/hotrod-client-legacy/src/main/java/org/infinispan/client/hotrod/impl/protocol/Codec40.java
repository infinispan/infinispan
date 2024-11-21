package org.infinispan.client.hotrod.impl.protocol;

import java.util.Map;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.impl.operations.GetWithMetadataOperation;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.marshall.Marshaller;

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
   public Object returnPossiblePrevValue(ByteBuf buf, short status, DataFormat dataFormat, int flags, ClassAllowList allowList, Marshaller marshaller) {
      if (HotRodConstants.hasPrevious(status)) {
         MetadataValue<Object> metadataValue = GetWithMetadataOperation.readMetadataValue(buf, status, dataFormat, allowList);
         return metadataValue != null ? metadataValue.getValue() : null;
      } else {
         return null;
      }
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

   @Override
   public int estimateSizeMultimapSupportsDuplicated() {
      return 1;
   }

   @Override
   public void writeMultimapSupportDuplicates(ByteBuf buf, boolean supportsDuplicates) {
      buf.writeByte(supportsDuplicates ? 1 : 0);
   }

   @Override
   public boolean isUnsafeForTheHandshake() {
      return true;
   }
}
