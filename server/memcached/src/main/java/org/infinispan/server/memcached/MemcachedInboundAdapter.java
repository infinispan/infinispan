package org.infinispan.server.memcached;

import java.util.concurrent.CompletionStage;

import org.infinispan.server.memcached.logging.Log;
import org.infinispan.server.memcached.logging.MemcachedAccessLogging;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.util.logging.LogFactory;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressiveFutureListener;
import io.netty.channel.ChannelProgressivePromise;
import io.netty.channel.ChannelPromise;
import io.netty.util.AttributeKey;

public class MemcachedInboundAdapter extends ChannelInboundHandlerAdapter {

   static final AttributeKey<ByteBufPool> ALLOCATOR_KEY = AttributeKey.valueOf("allocator");
   protected static final int MINIMUM_BUFFER_SIZE;

   protected static final Log log = LogFactory.getLog(MemcachedInboundAdapter.class, Log.class);

   private final MemcachedBaseDecoder decoder;

   protected ByteBuf outbound;
   protected ChannelProgressivePromise progressive;

   private long progressiveStart;
   private long progressiveEnd;

   // Variable to resume auto read when channel can be written to again. Some commands may resume themselves after
   // flush and may not want to also resume on writability changes
   protected boolean resumeAutoReadOnWritability;

   static {
      MINIMUM_BUFFER_SIZE = Integer.parseInt(System.getProperty("infinispan.memcached.minimum-buffer-size", "4096"));
   }

   public MemcachedInboundAdapter(MemcachedBaseDecoder decoder) {
      this.decoder = decoder;
   }

   public static ByteBufPool getAllocator(ChannelHandlerContext ctx) {
      ByteBufPool allocator = ctx.channel().attr(ALLOCATOR_KEY).get();
      if (allocator == null) throw new IllegalStateException("Context does not have a buffer allocator");
      return allocator;
   }

   private void flushBufferIfNeeded(ChannelHandlerContext ctx, boolean runOnEventLoop, MemcachedResponse res) {
      if (outbound != null) {
         if (runOnEventLoop) {
            ctx.channel().eventLoop().execute(() -> {
               ChannelPromise p = ctx.newPromise();
               ctx.writeAndFlush(outbound, p);

               if (res != null && progressive != null) {
                  res.flushed(p);
               }

               notifyPendingResponses(ctx, p);
               outbound = null;
            });
         } else {
            ChannelPromise p = ctx.newPromise();
            ctx.writeAndFlush(outbound, p);

            if (res != null && progressive != null) {
               res.flushed(p);
            }

            notifyPendingResponses(ctx, p);
            outbound = null;
         }
      }
   }

   public void flushBufferIfNeeded(ChannelHandlerContext ctx) {
      assert ctx.channel().eventLoop().inEventLoop();

      flushBufferIfNeeded(ctx, false, null);
   }

   private void notifyPendingResponses(ChannelHandlerContext ctx, ChannelFuture future) {
      assert ctx.channel().eventLoop().inEventLoop();
      if (progressive == null) return;
      if (progressiveEnd == progressiveStart) return;

      final long s = progressiveStart;
      final long e = progressiveEnd;
      future.addListener(ignore -> progressive.tryProgress(s, e));
      progressiveStart = progressiveEnd;
   }

   private ByteBuf allocateBuffer(ChannelHandlerContext ctx, int size) {
      if (outbound != null) {
         if (outbound.writableBytes() > size) {
            return outbound;
         }
         ctx.write(outbound, ctx.voidPromise());
      }
      int allocatedSize = Math.max(size, MINIMUM_BUFFER_SIZE);
      outbound = ctx.alloc().buffer(allocatedSize, allocatedSize);
      return outbound;
   }

   private void resumeAutoRead(ChannelHandlerContext ctx) {
      ctx.channel().config().setAutoRead(true);
      decoder.resumeRead();
   }

   public void handleExceptionally(ChannelHandlerContext ctx, MemcachedResponse response) {
      assert !response.isSuccessful();

      ByteBufPool allocator = ctx.channel().attr(ALLOCATOR_KEY).get();
      response.writeFailure(allocator);
      registerResponseForLater(response);
   }

   @Override
   public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
      ctx.channel().attr(ALLOCATOR_KEY).set(size -> allocateBuffer(ctx, size));
      progressive = MemcachedAccessLogging.isEnabled() ? ctx.newProgressivePromise() : null;
      super.handlerAdded(ctx);
   }

   @Override
   public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
      if (progressive != null) progressive.setSuccess();
      super.channelUnregistered(ctx);
   }

   @Override
   public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
      if (ctx.channel().config().isAutoRead()) {
         flushBufferIfNeeded(ctx);
      }
      super.channelReadComplete(ctx);
   }

   @Override
   public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
      if (resumeAutoReadOnWritability && ctx.channel().isWritable()) {
         resumeAutoReadOnWritability = false;
         resumeAutoRead(ctx);
      }
      super.channelWritabilityChanged(ctx);
   }

   @Override
   public void channelRead(ChannelHandlerContext ctx, Object msg) {
      if (msg == null) return;
      handleResponse(ctx, (MemcachedResponse) msg);
   }

   @Override
   public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      log.unexpectedException(cause);
      if (ctx.channel().isOpen()) flushBufferIfNeeded(ctx);
      ctx.close();
   }

   private void handleResponse(ChannelHandlerContext ctx, MemcachedResponse res) {
      CompletionStage<?> cs = res.getResponse();
      if (CompletionStages.isCompletedSuccessfully(cs)) {
         Object result = CompletionStages.join(cs);
         res.writeResponse(result, ctx.channel().attr(ALLOCATOR_KEY).get());
         if (outbound != null && outbound.readableBytes() > ctx.channel().bytesBeforeUnwritable()) {
            flushBufferIfNeeded(ctx, true, res);
            ctx.channel().config().setAutoRead(false);
            resumeAutoReadOnWritability = true;
            return;
         }
         registerResponseForLater(res);
         return;
      }

      ctx.channel().config().setAutoRead(false);
      cs.whenCompleteAsync((obj, t) -> {
         assert ctx.channel().eventLoop().inEventLoop();

         ByteBufPool allocator = ctx.channel().attr(ALLOCATOR_KEY).get();
         if (t != null) {
            res.writeFailure(t, allocator);
            flushBufferIfNeeded(ctx, false, res);
            return;
         }

         res.writeResponse(obj, allocator);
         flushBufferIfNeeded(ctx, false, res);
         resumeAutoRead(ctx);
      }, ctx.channel().eventLoop());
   }

   private void registerResponseForLater(MemcachedResponse res) {
      if (progressive != null) {
         progressive.addListener(new MemcachedProgressiveListener(res, progressiveEnd++));
      }
   }

   private static class MemcachedProgressiveListener implements ChannelProgressiveFutureListener {
      private final MemcachedResponse res;
      private final long id;

      private MemcachedProgressiveListener(MemcachedResponse res, long offset) {
         this.res = res;
         this.id = offset;
      }

      @Override
      public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) {
         if (id >= progress && id <= total) {
            res.flushed(future.channel().newSucceededFuture());
            future.removeListener(this);
         }
      }

      @Override
      public void operationComplete(ChannelProgressiveFuture future) {
         res.flushed(future);
      }
   }
}
