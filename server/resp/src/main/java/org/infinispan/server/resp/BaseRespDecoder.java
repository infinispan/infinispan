package org.infinispan.server.resp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.server.resp.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public abstract class BaseRespDecoder extends ByteToMessageDecoder {
   protected static final Log log = LogFactory.getLog(BaseRespDecoder.class, Log.class);
   protected final Intrinsics.Resp2LongProcessor longProcessor = new Intrinsics.Resp2LongProcessor();
   protected final int maxContentLength;
   // And this is the ByteBuf pos before decode is performed
   protected int posBefore;

   protected ChannelHandlerContext ctx;

   protected BaseRespDecoder(RespServer respServer) {
      maxContentLength = respServer != null ? respServer.getConfiguration().maxContentLengthBytes() : -1;
   }

   protected <T> List<T> allocList(int size) {
      return size == 0 ? Collections.emptyList() : new ArrayList<>(size);
   }

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
