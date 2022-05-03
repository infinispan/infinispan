package org.infinispan.hotrod.impl.operations;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.hotrod.impl.cache.VersionedMetadataImpl;
import org.infinispan.hotrod.impl.protocol.ChannelInputStream;
import org.infinispan.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.hotrod.impl.transport.netty.HeaderDecoder;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Streaming Get operation
 *
 * @since 14.0
 */
public class GetStreamOperation<K> extends AbstractKeyOperation<K, ChannelInputStream> {
   private final int offset;
   private Channel channel;

   public GetStreamOperation(OperationContext operationContext, K key, byte[] keyBytes, int offset, CacheOptions options) {
      super(operationContext, GET_STREAM_REQUEST, GET_STREAM_RESPONSE, key, keyBytes, options, null);
      this.offset = offset;
   }

   @Override
   public void executeOperation(Channel channel) {
      this.channel = channel;
      scheduleRead(channel);

      ByteBuf buf = channel.alloc().buffer(operationContext.getCodec().estimateHeaderSize(header) + ByteBufUtil.estimateArraySize(keyBytes)
            + ByteBufUtil.estimateVIntSize(offset));

      operationContext.getCodec().writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, keyBytes);
      ByteBufUtil.writeVInt(buf, offset);
      channel.writeAndFlush(buf);
   }

   @Override
   public void acceptResponse(ByteBuf buf, short status, HeaderDecoder decoder) {
      if (HotRodConstants.isNotExist(status) || !HotRodConstants.isSuccess(status)) {
         statsDataRead(false);
         complete(null);
      } else {
         short flags = buf.readUnsignedByte();
         long creation = -1;
         int lifespan = -1;
         long lastUsed = -1;
         int maxIdle = -1;
         if ((flags & INFINITE_LIFESPAN) != INFINITE_LIFESPAN) {
            creation = buf.readLong();
            lifespan = ByteBufUtil.readVInt(buf);
         }
         if ((flags & INFINITE_MAXIDLE) != INFINITE_MAXIDLE) {
            lastUsed = buf.readLong();
            maxIdle = ByteBufUtil.readVInt(buf);
         }
         long version = buf.readLong();
         int totalLength = ByteBufUtil.readVInt(buf);
         VersionedMetadataImpl versionedMetadata = new VersionedMetadataImpl(creation, lifespan, lastUsed, maxIdle, version);

         ChannelInputStream stream = new ChannelInputStream(versionedMetadata, () -> {
            // ChannelInputStreams removes itself when it finishes reading all data
            if (channel.pipeline().get(ChannelInputStream.class) != null) {
               channel.pipeline().remove(ChannelInputStream.class);
            }
         }, totalLength);
         if (stream.moveReadable(buf)) {
            channel.pipeline().addBefore(HeaderDecoder.NAME, ChannelInputStream.NAME, stream);
         }
         statsDataRead(true);
         complete(stream);
      }
   }
}
