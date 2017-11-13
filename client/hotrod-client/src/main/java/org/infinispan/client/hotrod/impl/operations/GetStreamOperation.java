package org.infinispan.client.hotrod.impl.operations;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.VersionedMetadataImpl;
import org.infinispan.client.hotrod.impl.protocol.ChannelInputStream;
import org.infinispan.client.hotrod.impl.protocol.Codec;
import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil;
import org.infinispan.client.hotrod.impl.transport.netty.ChannelFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import net.jcip.annotations.Immutable;

/**
 * Streaming Get operation
 *
 * @author Tristan Tarrant
 * @since 9.0
 */
@Immutable
public class GetStreamOperation extends AbstractKeyOperation<ChannelInputStream> {
   private final int offset;
   private Channel channel;

   public GetStreamOperation(Codec codec, ChannelFactory channelFactory,
                             Object key, byte[] keyBytes, int offset, byte[] cacheName, AtomicInteger topologyId, int flags, Configuration cfg) {
      super(codec, channelFactory, key, keyBytes, cacheName, topologyId, flags, cfg);
      this.offset = offset;
   }

   @Override
   public void executeOperation(Channel channel) {
      this.channel = channel;
      HeaderParams header = headerParams(GET_STREAM_REQUEST);
      scheduleRead(channel, header);

      ByteBuf buf = channel.alloc().buffer(codec.estimateHeaderSize(header) + ByteBufUtil.estimateArraySize(keyBytes)
            + ByteBufUtil.estimateVIntSize(offset));

      codec.writeHeader(buf, header);
      ByteBufUtil.writeArray(buf, keyBytes);
      ByteBufUtil.writeVInt(buf, offset);
      channel.writeAndFlush(buf);
   }

   @Override
   public void releaseChannel(Channel channel) {
      // noop
   }

   @Override
   public ChannelInputStream decodePayload(ByteBuf buf, short status) {
      if (HotRodConstants.isNotExist(status) || !HotRodConstants.isSuccess(status)) {
         super.releaseChannel(channel);
         return null;
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

         ChannelInputStream stream = new ChannelInputStream(versionedMetadata, () -> channelFactory.releaseChannel(channel), totalLength);
         channel.pipeline().addLast(stream);
         return stream;
      }
   }
}
