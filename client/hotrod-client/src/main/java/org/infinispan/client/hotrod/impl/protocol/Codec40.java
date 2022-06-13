package org.infinispan.client.hotrod.impl.protocol;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.impl.operations.GetWithMetadataOperation;
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
}
