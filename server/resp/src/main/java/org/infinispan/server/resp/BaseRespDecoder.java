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
   protected final static int MINIMUM_BUFFER_SIZE;
   protected final Intrinsics.Resp2LongProcessor longProcessor = new Intrinsics.Resp2LongProcessor();
   protected RespRequestHandler requestHandler;

   protected ByteBuf outboundBuffer;
   // Variable to resume auto read when channel can be written to again. Some commands may resume themselves after
   // flush and may not want to also resume on writability changes
   protected boolean resumeAutoReadOnWritability;

   static {
      MINIMUM_BUFFER_SIZE = Integer.parseInt(System.getProperty("infinispan.resp.minimum-buffer-size", "4096"));
   }

   protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, int size) {
      if (outboundBuffer != null) {
         if (outboundBuffer.writableBytes() > size) {
            return outboundBuffer;
         }
         log.tracef("Writing buffer %s as request is larger than remaining", outboundBuffer);
         ctx.write(outboundBuffer, ctx.voidPromise());
      }
      int allocatedSize = Math.max(size, MINIMUM_BUFFER_SIZE);
      outboundBuffer = ctx.alloc().buffer(allocatedSize, allocatedSize);
      return outboundBuffer;
   }

   private void flushBufferIfNeeded(ChannelHandlerContext ctx, boolean runOnEventLoop) {
      if (outboundBuffer != null) {
         log.tracef("Writing and flushing buffer %s", outboundBuffer);
         if (runOnEventLoop) {
            ctx.channel().eventLoop().execute(() -> {
               ctx.writeAndFlush(outboundBuffer, ctx.voidPromise());
               outboundBuffer = null;
            });
         } else {
            ctx.writeAndFlush(outboundBuffer, ctx.voidPromise());
            outboundBuffer = null;
         }
      }
   }

   @Override
   public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
      ctx.channel().attr(RespRequestHandler.BYTE_BUF_POOL_ATTRIBUTE_KEY)
            .set(size -> allocateBuffer(ctx, size));
      super.channelRegistered(ctx);
   }

   @Override
   public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
      super.channelUnregistered(ctx);
      requestHandler.handleChannelDisconnect(ctx);
   }

   @Override
   public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
      // We may disable auto read until a command completes or flush. This is useful because some operations may need
      // do a flush and then resume auto read. If the flush caused writability change we don't want to resume twice
      if (ctx.channel().config().isAutoRead()) {
         flushBufferIfNeeded(ctx, false);
      }
      super.channelReadComplete(ctx);
   }

   @Override
   public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
      if (resumeAutoReadOnWritability && ctx.channel().isWritable()) {
         resumeAutoReadOnWritability = false;
         // Schedule a read resume after we are done to prevent stack overflow
         ctx.channel().eventLoop().execute(() -> attemptReadResume(ctx));
      }
      super.channelWritabilityChanged(ctx);
   }

   protected void attemptReadResume(ChannelHandlerContext ctx) {
      log.tracef("Re-enabling auto read for channel %s as previous command is complete", ctx.channel());
      ctx.channel().config().setAutoRead(true);
      // If there is any readable bytes left before we paused make sure to try to decode, just in case
      // if a pending message was read before we disabled auto read
      ByteBuf buf = internalBuffer();
      if (buf.isReadable()) {
         log.tracef("Bytes available from previous read for channel %s, trying decode directly", ctx.channel());
         // callDecode will call us until the ByteBuf is no longer consumed
         callDecode(ctx, buf, List.of());
         // It is possible the decode above filled our outbound buffer again, so we can only flush if auto read is still enabled
         if (ctx.channel().config().isAutoRead()) {
            flushBufferIfNeeded(ctx, false);
         }
      }
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
   protected boolean handleCommandAndArguments(ChannelHandlerContext ctx, RespCommand command, List<byte[]> arguments) {
      if (log.isTraceEnabled()) {
         log.tracef("Received command: %s with arguments %s for %s", command, Util.toStr(arguments), ctx.channel());
      }

      CompletionStage<RespRequestHandler> stage = requestHandler.handleRequest(ctx, command, arguments);
      if (CompletionStages.isCompletedSuccessfully(stage)) {
         requestHandler = CompletionStages.join(stage);
         if (outboundBuffer != null && outboundBuffer.readableBytes() > ctx.channel().bytesBeforeUnwritable()) {
            log.tracef("Buffer will cause channel %s to be unwriteable - forcing flush", ctx.channel());
            // Note the flush is done later after this task completes, since we don't want to resume reading yet
            flushBufferIfNeeded(ctx, true);
            ctx.channel().config().setAutoRead(false);
            resumeAutoReadOnWritability = true;
            return false;
         }
         return true;
      }
      log.tracef("Disabling auto read for channel %s until previous command is complete", ctx.channel());
      // Disable reading any more from socket - until command is complete
      ctx.channel().config().setAutoRead(false);
      stage.whenComplete((handler, t) -> {
         assert ctx.channel().eventLoop().inEventLoop();
         if (t != null) {
            exceptionCaught(ctx, t);
            return;
         }
         // Instate the new handler if there was no exception
         requestHandler = handler;
         flushBufferIfNeeded(ctx, false);
         // Schedule a read resume after we are done to prevent stack overflow
         ctx.channel().eventLoop().execute(() -> attemptReadResume(ctx));
      });
      return false;
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      log.unexpectedException(cause);
      RespRequestHandler.stringToByteBuf("-ERR Server Error Encountered: " + cause.getMessage() + "\\r\\n", requestHandler.allocatorToUse);
      flushBufferIfNeeded(ctx, false);
      ctx.close();
   }
}
