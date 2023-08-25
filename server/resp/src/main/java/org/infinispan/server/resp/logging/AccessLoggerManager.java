package org.infinispan.server.resp.logging;

import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.function.IntConsumer;

import org.infinispan.commons.time.TimeService;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.util.concurrent.CompletionStages;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelProgressiveFuture;
import io.netty.channel.ChannelProgressiveFutureListener;
import io.netty.channel.ChannelProgressivePromise;

/**
 * Handle the access log.
 * <p>
 * Keeps track of pending operations and pushes them to the access log. Some operations are not flushed directly
 * because of the pipelining, so the manager keeps them with {@link ChannelProgressivePromise}. Once the buffer is
 * flushed, the {@link ChannelProgressivePromise} advances the indexes and flushes the pending operations to the
 * access log.
 * <p>
 * The manager tracks data of only one active operation at a time. The current active operation is then flushed or
 * registered to do so later. This approach fits with the way we handle the commands on the
 * {@link org.infinispan.server.resp.RespHandler}.
 * <p>
 * <b>Thread safety</b>: This implementation is <b>not</b> thread-safe. It assumes all invocations come from the same
 * thread to track, register, and flush operations.
 *
 * @since 15.0
 */
public class AccessLoggerManager implements IntConsumer {
   private final ChannelProgressivePromise promise;
   private final Tracker tracker;
   private long start;
   private long end;

   public AccessLoggerManager(ChannelHandlerContext ctx, TimeService timeService) {
      this.promise = ctx.newProgressivePromise();
      this.tracker = new Tracker(ctx, timeService);
   }

   public void track(RespCommand req, List<byte[]> arguments) {
      // Unknown command.
      if (req == null) return;

      tracker.track(req, arguments);
   }

   public void flush(ChannelHandlerContext ctx, ChannelFuture future, CompletionStage<?> pending) {
      assert ctx.channel().eventLoop().inEventLoop() : "Flush must happen from event loop";

      if (end > start) {
         final long s = start;
         final long e = end;
         future.addListener(ignore -> {
            promise.tryProgress(s, e);
         });
         start = end;
      }

      if (pending != null) logCompleted(future, pending);
   }

   public void register(CompletionStage<?> res) {
      if (CompletionStages.isCompletedSuccessfully(res)) {
         registerFinishedOperation(null);
         return;
      }

      res.whenComplete((ignore, t) -> registerFinishedOperation(t));
   }

   public void close() {
      promise.setSuccess();
   }

   private void logCompleted(ChannelFuture future, CompletionStage<?> pending) {
      if (CompletionStages.isCompletedSuccessfully(pending)) {
         AccessData data = tracker.done(null);
         if (data != null) data.log(future);
         return;
      }

      pending.whenComplete((ignore, t) -> {
         AccessData data = tracker.done(t);
         if (data != null) data.log(future);
      });
   }

   private void registerFinishedOperation(Throwable t) {
      AccessData data = tracker.done(t);

      if (data == null)
         throw new IllegalStateException("No operation tracked!");

      promise.addListener(new LogProgressiveListener(data, end++));
   }

   @Override
   public void accept(int value) {
      // Bytes requested to buffer allocator.
      tracker.increaseBytesRequested(value);
   }

   private static class LogProgressiveListener implements ChannelProgressiveFutureListener {
      private final AccessData data;
      private final long offset;

      private LogProgressiveListener(AccessData data, long offset) {
         this.data = data;
         this.offset = offset;
      }

      @Override
      public void operationProgressed(ChannelProgressiveFuture future, long progress, long total) {
         if (offset >= progress && offset <= total) {
            data.log(future.channel().newSucceededFuture());
            future.removeListener(this);
         }
      }

      @Override
      public void operationComplete(ChannelProgressiveFuture future) {
         data.log(future);
      }
   }
}
