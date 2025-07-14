package org.infinispan.client.hotrod.impl.operations;

import org.infinispan.client.hotrod.exceptions.TransportException;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class PutStreamNextOperation extends AbstractCacheOperation<Boolean> {
   private final int id;
   private final boolean lastChunk;
   private final ByteBuf valueBytes;
   private final Channel channel;

   protected PutStreamNextOperation(InternalRemoteCache<?, ?> internalRemoteCache, int id, boolean lastChunk,
                                    ByteBuf valueBytes, Channel channel) {
      super(internalRemoteCache);
      this.id = id;
      this.lastChunk = lastChunk;
      this.valueBytes = valueBytes;
      this.channel = channel;
   }

   @Override
   public void writeOperationRequest(Channel channel, ByteBuf buf, Codec codec) {
      if (channel != this.channel) {
         throw new TransportException("PutStreamNextOperation must be performed on the same Channel", channel.remoteAddress());
      }
      buf.writeInt(id);
      buf.writeBoolean(lastChunk);
      ByteBufUtil.writeVInt(buf, valueBytes.readableBytes());
      buf.writeBytes(valueBytes);
      // We don't support retry so just release immediately after writing
      valueBytes.release();
   }

   @Override
   public Boolean createResponse(ByteBuf buf, short status, HeaderDecoder decoder, Codec codec, CacheUnmarshaller unmarshaller) {
      return HotRodConstants.isSuccess(status);
   }

   @Override
   public short requestOpCode() {
      return HotRodConstants.NEXT_PUT_STREAM_REQUEST;
   }

   @Override
   public short responseOpCode() {
      return HotRodConstants.NEXT_PUT_STREAM_RESPONSE;
   }

   @Override
   public boolean supportRetry() {
      return false;
   }
}
