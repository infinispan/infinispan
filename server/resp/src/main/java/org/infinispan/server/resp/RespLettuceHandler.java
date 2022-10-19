package org.infinispan.server.resp;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.util.Util;
import org.infinispan.server.resp.logging.Log;
import org.infinispan.util.concurrent.CompletionStages;

import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.output.PushOutput;
import io.lettuce.core.protocol.RedisStateMachine;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class RespLettuceHandler extends ByteToMessageDecoder {
   private final static Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass(), Log.class);

   private final RedisStateMachine stateMachine = new RedisStateMachine(ByteBufAllocator.DEFAULT);
   private RespRequestHandler requestHandler;
   private boolean disabledRead = false;

   public RespLettuceHandler(RespRequestHandler initialHandler) {
      this.requestHandler = initialHandler;
   }

   @Override
   public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
      super.channelUnregistered(ctx);
      requestHandler.handleChannelDisconnect(ctx);
   }

   @Override
   protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
      // Don't read any of the ByteBuf if we disabled reads
      if (disabledRead) {
         return;
      }
      PushOutput<byte[], byte[]> pushOutput = new PushOutput<>(ByteArrayCodec.INSTANCE);
      if (stateMachine.decode(in, pushOutput)) {
         String type = pushOutput.getType().toUpperCase();
         List content = pushOutput.getContent();
         List<byte[]> contentToUse = content.subList(1, content.size());
         log.tracef("Received command: %s with arguments %s", type, Util.toStr(contentToUse));
         CompletionStage<RespRequestHandler> stage = requestHandler.handleRequest(ctx, type, contentToUse);
         if (CompletionStages.isCompletedSuccessfully(stage)) {
            requestHandler = CompletionStages.join(stage);
         } else {
            log.tracef("Disabling auto read for channel %s until previous command is complete", ctx.channel());
            // Disable reading any more from socket - until command is complete
            ctx.channel().config().setAutoRead(false);
            disabledRead = true;
            stage.whenComplete((handler, t) -> {
               assert ctx.channel().eventLoop().inEventLoop();
               log.tracef("Re-enabling auto read for channel %s as previous command is complete", ctx.channel());
               ctx.channel().config().setAutoRead(true);
               disabledRead = false;
               if (t != null) {
                  exceptionCaught(ctx, t);
               } else {
                  // Instate the new handler if there was no exception
                  requestHandler = handler;
               }

               // If there is any readable bytes left before we paused make sure to try to decode, just in case
               // if a pending message was read before we disabled auto read
               ByteBuf buf = internalBuffer();
               if (buf.isReadable()) {
                  log.tracef("Bytes available from previous read for channel %s, trying decode directly", ctx.channel());
                  // callDecode will call us until the ByteBuf is no longer consumed
                  callDecode(ctx, buf, List.of());
               }
            });
         }
      }
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      log.unexpectedException(cause);
      ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("-ERR Server Error Encountered: " + cause.getMessage() + "\r\n", ctx.alloc()));
      ctx.close();
   }
}
