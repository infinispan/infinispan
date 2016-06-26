package org.infinispan.server.core.transport;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufHolder;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

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

   public StatsChannelHandler(NettyTransport transport) {
      this.transport = transport;
   }

   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      transport.updateTotalBytesRead(getByteSize(msg));
      super.channelRead(ctx, msg);
   }

   @Override
   public void channelActive(ChannelHandlerContext ctx) throws Exception {
      transport.acceptedChannels.add(ctx.channel());
      super.channelActive(ctx);
   }

   @Override
   public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      int writable = getByteSize(msg);

      transport.updateTotalBytesWritten(writable);
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
