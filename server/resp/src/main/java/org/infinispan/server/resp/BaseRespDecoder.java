package org.infinispan.server.resp;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.Util;
import org.infinispan.server.resp.logging.Log;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public abstract class BaseRespDecoder extends ByteToMessageDecoder {
   protected final static Log log = LogFactory.getLog(RespDecoder.class, Log.class);
   protected final Intrinsics.Resp2LongProcessor longProcessor = new Intrinsics.Resp2LongProcessor();
   protected RespRequestHandler requestHandler;

   @Override
   public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
      super.channelUnregistered(ctx);
      requestHandler.handleChannelDisconnect(ctx);
   }

   /**
    * Handles the actual command request. This entails passing the command to the request handler and if
    * the request is completed the decoder may parse more commands.
    *
    * @param ctx channel context in use for this command
    * @param command the actual command
    * @param arguments the arguments provided to the command. The list should not be retained as it is reused
    * @return boolean whether the decoder can read more bytes or must wait
    */
   protected boolean handleCommandAndArguments(ChannelHandlerContext ctx, String command, List<byte[]> arguments) {
      if (log.isTraceEnabled()) {
         log.tracef("Received command: %s with arguments %s for %s", command, Util.toStr(arguments), ctx.channel());
      }

      CompletionStage<RespRequestHandler> stage = requestHandler.handleRequest(ctx, command, arguments);
      if (CompletionStages.isCompletedSuccessfully(stage)) {
         requestHandler = CompletionStages.join(stage);
         return true;
      }
      log.tracef("Disabling auto read for channel %s until previous command is complete", ctx.channel());
      // Disable reading any more from socket - until command is complete
      ctx.channel().config().setAutoRead(false);
      stage.whenComplete((handler, t) -> {
         assert ctx.channel().eventLoop().inEventLoop();
         log.tracef("Re-enabling auto read for channel %s as previous command is complete", ctx.channel());
         ctx.channel().config().setAutoRead(true);
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
      return false;
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      log.unexpectedException(cause);
      ctx.writeAndFlush(RespRequestHandler.stringToByteBuf("-ERR Server Error Encountered: " + cause.getMessage() + "\\r\\n", ctx.alloc()));
      ctx.close();
   }
}
