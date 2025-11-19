package org.infinispan.server.hotrod;

import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.server.hotrod.logging.Log;
import org.infinispan.server.hotrod.streaming.GetStreamResponse;
import org.infinispan.server.hotrod.transport.ExtendedByteBuf;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

public class Encoder4x extends Encoder2x {
   private static final Log log = Log.getLog(Encoder2x.class);
   private static final Encoder4x INSTANCE = new Encoder4x();

   public static Encoder4x instance() {
      return INSTANCE;
   }

   @Override
   public ByteBuf valueResponse(HotRodHeader header, HotRodServer server, Channel channel, OperationStatus status, CacheEntry<byte[], byte[]> prev) {
      ByteBuf buf = writeMetadataResponse(header, server, channel, status, prev);
      if (log.isTraceEnabled()) {
         log.tracef("Write response to %s messageId=%d status=%s prev=%s", header.op, header.messageId, status, Util.printArray(prev != null ? prev.getValue() : null));
      }
      return buf;
   }

   @Override
   public ByteBuf getStreamStartResponse(HotRodHeader header, HotRodServer server, Channel channel,
                                         CacheEntry<?, ?> entry, GetStreamResponse getStreamResponse) {
      assert getStreamResponse.value() != null;
      ByteBuf buf = writeHeader(header, server, channel, OperationStatus.Success);
      buf.writeInt(getStreamResponse.id());
      buf.writeBoolean(getStreamResponse.complete());
      if (entry != null) {
         MetadataUtils.writeMetadata(MetadataUtils.extractLifespan(entry), MetadataUtils.extractMaxIdle(entry),
               MetadataUtils.extractCreated(entry), MetadataUtils.extractLastUsed(entry), MetadataUtils.extractVersion(entry), buf);
      }
      ByteBuf result = getStreamResponse.value();
      ExtendedByteBuf.writeUnsignedInt(result.readableBytes(), buf);
      // Write the header and other values ByteBuf so can finish with just our value in the return
      channel.write(buf, channel.voidPromise());
      return result;
   }

   @Override
   public ByteBuf putStreamStartResponse(HotRodHeader header, HotRodServer server, Channel channel, int id) {
      ByteBuf buf = writeHeader(header, server, channel, OperationStatus.Success);
      buf.writeInt(id);
      return buf;
   }
}
