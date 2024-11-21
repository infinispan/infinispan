package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public abstract class AbstractKeyOperation<V> extends AbstractCacheOperation<V> {
   protected final byte[] keyBytes;

   protected AbstractKeyOperation(InternalRemoteCache<?, ?> internalRemoteCache, byte[] keyBytes) {
      super(internalRemoteCache);
      this.keyBytes = keyBytes;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      ByteBufUtil.writeArray(buf, keyBytes);
   }

   @Override
   public Object getRoutingObject() {
      return keyBytes;
   }

   @SuppressWarnings("unchecked")
   protected <T> T returnPossiblePrevValue(ByteBuf buf, short status, Codec codec, CacheUnmarshaller unmarshaller) {
      return (T) codec.returnPossiblePrevValue(buf, status, unmarshaller);
   }

   protected <T> MetadataValue<T> returnMetadataValue(ByteBuf buf, short status, Codec codec, CacheUnmarshaller unmarshaller) {
      return codec.returnMetadataValue(buf, status, unmarshaller);
   }

   protected <E> VersionedOperationResponse<E> returnVersionedOperationResponse(ByteBuf buf, short status, Codec codec,
                                                                         CacheUnmarshaller unmarshaller) {
      VersionedOperationResponse.RspCode code;
      if (HotRodConstants.isSuccess(status)) {
         code = VersionedOperationResponse.RspCode.SUCCESS;
      } else if (HotRodConstants.isNotExecuted(status)) {
         code = VersionedOperationResponse.RspCode.MODIFIED_KEY;
      } else if (HotRodConstants.isNotExist(status)) {
         code = VersionedOperationResponse.RspCode.NO_SUCH_KEY;
      } else {
         throw new IllegalStateException("Unknown response status: " + Integer.toHexString(status));
      }
      MetadataValue<E> metadata = returnMetadataValue(buf, status, codec, unmarshaller);
      return new VersionedOperationResponse<>(metadata, code);
   }
}
