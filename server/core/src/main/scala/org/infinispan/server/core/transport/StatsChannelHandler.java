package org.infinispan.server.core.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;

import java.time.Instant;

/**
 * Input/Output ChannelHandler to keep statistics
 *
 * @author gustavonalle
 * @author wburns
 * @since 7.1
 */
public class StatsChannelHandler extends ChannelDuplexHandler {
   private final NettyTransport transport;
   public static AttributeKey<Integer> bytesRead = AttributeKey.valueOf("__bytesRead");
   public static AttributeKey<Instant> startInstant = AttributeKey.valueOf("__startInstant");

   public StatsChannelHandler(NettyTransport transport) {
      this.transport = transport;
   }

   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      int readable = getByteSize(msg);

      Attribute<Integer> count = ctx.channel().attr(bytesRead);
      count.set(readable);
      Attribute<Instant> start = ctx.channel().attr(startInstant);
      start.set(Instant.now());

      transport.updateTotalBytesRead(readable);
      super.channelRead(ctx, msg);
   }

   @Override
   public void channelActive(ChannelHandlerContext ctx) throws Exception {
      transport.acceptedChannels().add(ctx.channel());
      super.channelActive(ctx);
   }

   @Override
   public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      transport.updateTotalBytesWritten(getByteSize(msg));
      super.write(ctx, msg, promise);
   }

   int getByteSize(Object msg) {
      if (msg instanceof ByteBuf) {
         return ((ByteBuf) msg).readableBytes();
      } else if (msg instanceof ByteBufHolder) {
         return ((ByteBufHolder) msg).content().readableBytes();
      } else {
         return -1;
      }
   }
}
