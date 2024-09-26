package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class GetStreamNextOperation extends AbstractCacheOperation<GetStreamNextResponse> {
   private final int id;
   private final Channel channel;

   protected GetStreamNextOperation(InternalRemoteCache<?, ?> internalRemoteCache, int id, Channel channel) {
      super(internalRemoteCache);
      this.id = id;
      this.channel = channel;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      if (channel != this.channel) {
         throw new TransportException("GetStreamNextOperation must be performed on the same Channel", channel.remoteAddress());
      }
      buf.writeInt(id);
   }

   @Override
   public GetStreamNextResponse createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      if (!HotRodConstants.isNotExist(status) && HotRodConstants.isSuccess(status)) {
         // Ignore the id
         int readId = buf.readInt();
         assert id == readId;
         boolean complete = buf.readBoolean();
         int length = ByteBufUtil.readVInt(buf);
         ByteBuf value = buf.readRetainedSlice(length);
         return new GetStreamNextResponse(value, complete);
      }
      return null;
   }

   @Override
   public short requestOpCode() {
      return HotRodConstants.NEXT_GET_STREAM_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return HotRodConstants.NEXT_GET_STREAM_RESPONSE;
   }

   @Override
   public boolean supportRetry() {
      return false;
   }
}
