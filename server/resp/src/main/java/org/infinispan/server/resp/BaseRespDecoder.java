package org.infinispan.server.resp;

import org.infinispan.server.resp.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public abstract class BaseRespDecoder extends ByteToMessageDecoder {
   protected final static Log log = LogFactory.getLog(BaseRespDecoder.class, Log.class);
   protected final Intrinsics.Resp2LongProcessor longProcessor = new Intrinsics.Resp2LongProcessor();

   protected ChannelHandlerContext ctx;

   @Override
   public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
      super.channelRegistered(ctx);
      this.ctx = ctx;
   }

   public void resumeRead() {
      // Also double check auto read in case if enabling auto read caused more bytes to be read which in turn disabled
      // auto read again
      if (internalBuffer().isReadable() && ctx.channel().config().isAutoRead()) {
         // Schedule the read for later to prevent possible StackOverflow
         ctx.channel().eventLoop().submit(() -> {
            try {
               // We HAVE to use our ctx otherwise a read may be in the wrong spot of the pipeline
               channelRead(ctx, Unpooled.EMPTY_BUFFER);
               channelReadComplete(ctx);
            } catch (Throwable t) {
               ctx.fireExceptionCaught(t);
            }
         });
      }
   }
}
