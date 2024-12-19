package org.infinispan.server.resp;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.infinispan.server.resp.logging.Log;
import org.infinispan.util.logging.LogFactory;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.TooLongFrameException;

public abstract class BaseRespDecoder extends ByteToMessageDecoder {
   protected final static Log log = LogFactory.getLog(BaseRespDecoder.class, Log.class);
   protected final Intrinsics.Resp2LongProcessor longProcessor = new Intrinsics.Resp2LongProcessor();
   protected final int maxArrayLength;
   protected final int maxKeyCount;

   protected ChannelHandlerContext ctx;

   protected BaseRespDecoder(RespServer respServer) {
      maxArrayLength = respServer != null ? respServer.getConfiguration().maxByteArraySize() : -1;
      maxKeyCount = respServer != null ? respServer.getConfiguration().maxKeyCount() : -1;
   }

   protected <T> List<T> allocList(int size, int maxKeyCount) {
      if (maxKeyCount > 0 && size > maxKeyCount) {
         throw new TooLongFrameException("List size " + size + " exceeded " + maxKeyCount);
      }
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
