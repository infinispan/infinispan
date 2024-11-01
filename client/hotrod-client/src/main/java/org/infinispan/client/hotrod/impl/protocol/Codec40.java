package org.infinispan.client.hotrod.impl.protocol;

import java.util.Map;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.impl.ClientTopology;
import org.infinispan.client.hotrod.impl.operations.CacheUnmarshaller;
import org.infinispan.client.hotrod.impl.operations.GetWithMetadataOperation;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;

import io.netty.buffer.ByteBuf;

/**
 * @since 14.0
 */
public class Codec40 extends Codec31 {
   @Override
   public void writeHeader(ByteBuf buf, long messageId, ClientTopology clientTopology, HotRodOperation<?> operation) {
      writeHeader(buf, messageId, clientTopology, operation, HotRodConstants.VERSION_40);
   }

   @Override
   public Object returnPossiblePrevValue(ByteBuf buf, short status, CacheUnmarshaller unmarshaller) {
      if (HotRodConstants.hasPrevious(status)) {
         MetadataValue<Object> metadataValue = GetWithMetadataOperation.readMetadataValue(buf, status, unmarshaller);
         return metadataValue != null ? metadataValue.getValue() : null;
      } else {
         return null;
      }
   }

   @Override
   public <V> MetadataValue<V> returnMetadataValue(ByteBuf buf, short status, CacheUnmarshaller unmarshaller) {
      if (!HotRodConstants.hasPrevious(status)) return null;

      return GetWithMetadataOperation.readMetadataValue(buf, status, unmarshaller);
   }

   @Override
   protected void writeHeader(ByteBuf buf, long messageId, ClientTopology clientTopology, HotRodOperation<?> operation, byte version) {
      super.writeHeader(buf, messageId, clientTopology, operation, version);
      writeOtherParams(buf, operation.additionalParameters());
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
   public void writeMultimapSupportDuplicates(ByteBuf buf, boolean supportsDuplicates) {
      buf.writeByte(supportsDuplicates ? 1 : 0);
   }

   @Override
   public boolean isUnsafeForTheHandshake() {
      return true;
   }
}
