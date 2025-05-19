package org.infinispan.server.resp;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.Util;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.server.resp.logging.AccessLoggerManager;
import org.infinispan.server.resp.logging.Log;
import org.infinispan.server.resp.logging.RespAccessLogger;
import org.infinispan.util.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.TooLongFrameException;

public class RespHandler extends ChannelInboundHandlerAdapter {
   protected static final Log log = LogFactory.getLog(RespHandler.class, Log.class);
   protected static final org.infinispan.server.core.logging.Log coreLog =
         LogFactory.getLog(RespHandler.class, org.infinispan.server.core.logging.Log.class);
   protected static final int MINIMUM_BUFFER_SIZE;

   protected final BaseRespDecoder resumeHandler;
   protected RespRequestHandler requestHandler;

   protected ByteBuf outboundBuffer;
   // Variable to resume auto read when channel can be written to again. Some commands may resume themselves after
   // flush and may not want to also resume on writability changes
   protected boolean resumeAutoReadOnWritability;

   private final boolean traceAccess = RespAccessLogger.isEnabled();
   private AccessLoggerManager accessLogger;

   static {
      MINIMUM_BUFFER_SIZE = Integer.parseInt(System.getProperty("infinispan.resp.minimum-buffer-size", "4096"));
   }

   public RespHandler(BaseRespDecoder resumeHandler, RespRequestHandler requestHandler) {
      this.resumeHandler = resumeHandler;
      this.requestHandler = requestHandler;
   }

   protected ByteBuf allocateBuffer(ChannelHandlerContext ctx, int size) {
      assert ctx.channel().eventLoop().inEventLoop() : "Buffer allocation should occur in event loop, it was " + Thread.currentThread().getName();
      if (traceAccess) accessLogger.accept(size);
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

   private void flushBufferIfNeeded(ChannelHandlerContext ctx, boolean runOnEventLoop, CompletionStage<?> res) {
      if (outboundBuffer != null) {
         log.tracef("Writing and flushing buffer %s", outboundBuffer);
         if (runOnEventLoop) {
            ctx.channel().eventLoop().execute(() -> {
               ChannelPromise p = newPromise(ctx);
               ctx.writeAndFlush(outboundBuffer, p);
               flushAccessLog(ctx, p, res);
               outboundBuffer = null;
            });
         } else {
            ChannelPromise p = newPromise(ctx);
            ctx.writeAndFlush(outboundBuffer, p);
            flushAccessLog(ctx, p, res);
            outboundBuffer = null;
         }
      }
   }

   private ChannelPromise newPromise(ChannelHandlerContext ctx) {
      return traceAccess ? ctx.newPromise() : ctx.voidPromise();
   }

   private void flushAccessLog(ChannelHandlerContext ctx, ChannelPromise promise, CompletionStage<?> res) {
      if (accessLogger == null) return;

      accessLogger.flush(ctx, promise, res);
   }

   @Override
   public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
      ctx.channel().attr(RespRequestHandler.BYTE_BUF_POOL_ATTRIBUTE_KEY)
            .set(size -> allocateBuffer(ctx, size));
      this.accessLogger = traceAccess
            ? new AccessLoggerManager(ctx, requestHandler.respServer().getTimeService())
            : null;
      requestHandler.respServer().metadataRepository().client().incrementConnectedClients();
      super.channelRegistered(ctx);
   }

   @Override
   public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
      super.channelUnregistered(ctx);
      requestHandler.handleChannelDisconnect(ctx);
      if (traceAccess) accessLogger.close();
      requestHandler.respServer().metadataRepository().client().decrementConnectedClients();
   }

   @Override
   public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
      // If we disabled auto read in the middle of a read, that means we are waiting on a pending command to complete
      if (ctx.channel().config().isAutoRead()) {
         flushBufferIfNeeded(ctx, false, null);
      }
      super.channelReadComplete(ctx);
   }

   @Override
   public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
      if (resumeAutoReadOnWritability && ctx.channel().isWritable()) {
         resumeAutoReadOnWritability = false;
         log.tracef("Re-enabling auto read for channel %s as channel is now writeable", ctx.channel());
         resumeAutoRead(ctx);
      }
      super.channelWritabilityChanged(ctx);
   }

   protected void resumeAutoRead(ChannelHandlerContext ctx) {
      ctx.channel().config().setAutoRead(true);
      resumeHandler.resumeRead();
   }

   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) {
      RespDecoder arg = (RespDecoder) msg;
      handleCommandAndArguments(ctx, arg.getCommand(), arg.getArguments());
   }

   /**
    * Handles the actual command request. This entails passing the command to the request handler and if
    * the request is completed the decoder may parse more commands.
    *
    * @param ctx channel context in use for this command
    * @param command the actual command
    * @param arguments the arguments provided to the command. The list should not be retained as it is reused
    */
   protected void handleCommandAndArguments(ChannelHandlerContext ctx, RespCommand command, List<byte[]> arguments) {
      if (log.isTraceEnabled()) {
         log.tracef("Received command: %s with arguments %s for %s", command, Util.toStr(arguments), ctx.channel());
      }

      if (traceAccess) accessLogger.track(command, arguments);

      CompletionStage<RespRequestHandler> stage = requestHandler.handleRequest(ctx, command, arguments);
      if (CompletionStages.isCompletedSuccessfully(stage)) {
         requestHandler = CompletionStages.join(stage);
         if (outboundBuffer != null && outboundBuffer.readableBytes() > ctx.channel().bytesBeforeUnwritable()) {
            log.tracef("Buffer will cause channel %s to be unwriteable - forcing flush", ctx.channel());
            // Note the flush is done later after this task completes, since we don't want to resume reading yet
            flushBufferIfNeeded(ctx, true, stage);
            ctx.channel().config().setAutoRead(false);
            resumeAutoReadOnWritability = true;
            return;
         }
         if (traceAccess) accessLogger.register(stage);

         return;
      }
      log.tracef("Disabling auto read for channel %s until previous command is complete", ctx.channel());
      // Disable reading any more from socket - until command is complete
      ctx.channel().config().setAutoRead(false);
      stage.whenComplete((handler, t) -> {
         assert ctx.channel().eventLoop().inEventLoop() : "Command should complete only in event loop thread, it was " + Thread.currentThread().getName();
         if (t != null) {
            requestHandler.writer.error(t);
         } else {
            requestHandler = handler; // Instate the new handler if there was no exception
         }
         flushBufferIfNeeded(ctx, false, stage);
         log.tracef("Re-enabling auto read for channel %s as previous command is complete", ctx.channel());
         resumeAutoRead(ctx);
      });
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      if (cause instanceof TooLongFrameException tlfe) {
         coreLog.requestTooLarge(ctx.channel(), tlfe);
      } else {
         log.unexpectedException(cause);
         requestHandler.writer.error(cause);
         flushBufferIfNeeded(ctx, false, null);
      }
      ctx.close();
   }
}
