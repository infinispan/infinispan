package org.infinispan.server.hotrod.transport;

import static org.infinispan.server.core.transport.ExtendedByteBuf.hexDump;

import java.net.SocketAddress;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.server.hotrod.logging.Log;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;

/**
 * Channel handler that logs every connect/disconnect/close/read/write event at trace level.
 *
 * @author Dan Berindei
 * @since 11
 */
public class TraceChannelHandler extends ChannelDuplexHandler {
   private static final Log log = LogFactory.getLog(TraceChannelHandler.class, Log.class);

   @Override
   public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress,
                       ChannelPromise promise) throws Exception {
      log.tracef("Channel %s connect", ctx.channel());
      super.connect(ctx, remoteAddress, localAddress, promise);
   }

   @Override
   public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
      log.tracef("%s disconnect", ctx.channel());
      super.disconnect(ctx, promise);
   }

   @Override
   public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception {
      log.tracef("%s close", ctx.channel());
      super.close(ctx, promise);
   }

   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
      String msgString = msg instanceof ByteBuf ? hexDump(((ByteBuf) msg)) : Util.toStr(msg);
      log.tracef("%s read: %s", ctx.channel(), msgString);
      super.channelRead(ctx, msg);
   }

   @Override
   public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
      String msgString = msg instanceof ByteBuf ? hexDump(((ByteBuf) msg)) : Util.toStr(msg);
      log.tracef("%s write: %s", ctx.channel(), msgString);
      super.write(ctx, msg, promise);
   }

}
