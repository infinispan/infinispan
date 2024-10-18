package org.infinispan.server.resp.commands.iteration;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.server.iteration.IterableIterationResult;
import org.infinispan.server.iteration.IterationInitializationContext;
import org.infinispan.server.iteration.IterationManager;
import org.infinispan.server.iteration.IterationState;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ByteBufferUtils;
import org.infinispan.server.resp.serialization.Resp3Response;
import org.infinispan.server.resp.serialization.Resp3Type;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.util.concurrent.BlockingManager;

import io.netty.channel.ChannelHandlerContext;

public abstract class BaseIterationCommand extends RespCommand implements Resp3Command {
   private static final String INITIAL_CURSOR = "0";

   protected BaseIterationCommand(int arity, int firstKeyPos, int lastKeyPos, int steps) {
      super(arity, firstKeyPos, lastKeyPos, steps);
   }

   protected byte[] getMatch(List<byte[]> arguments) {
      return null;
   }

   @Override
   public final CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      IterationArguments args = IterationArguments.parse(handler, arguments, getMatch(arguments));
      if (args == null) return handler.myStage();

      IterationManager manager = retrieveIterationManager(handler);
      String cursor = cursor(arguments);

      if (INITIAL_CURSOR.equals(cursor)) {
         CompletionStage<IterationInitializationContext> initialization = initializeIteration(handler, arguments);
         if (initialization != null) {
            return handler.stageToReturn(initialization.thenCompose(iic -> initializeAndIterate(handler, ctx, manager, args, iic)), ctx);
         }
         return initializeAndIterate(handler, ctx, manager, args, null);
      }

      return iterate(handler, ctx, manager, cursor, args);
   }

   private CompletionStage<RespRequestHandler> initializeAndIterate(Resp3Handler handler, ChannelHandlerContext ctx,
                                                                    IterationManager manager, IterationArguments arguments,
                                                                    IterationInitializationContext iic) {
      AdvancedCache<Object, Object> cache = handler.cache().withMediaType(MediaType.APPLICATION_OCTET_STREAM, null);
      IterationState iterationState = manager.start(cache, null, arguments.getFilterConverterFactory(),
            arguments.getFilterConverterParams(), null, arguments.getCount(),
            false, DeliveryGuarantee.AT_LEAST_ONCE, iic);
      iterationState.getReaper().registerChannel(ctx.channel());

      return iterate(handler, ctx, manager, iterationState.getId(), arguments);
   }

   private CompletionStage<RespRequestHandler> iterate(Resp3Handler handler, ChannelHandlerContext ctx, IterationManager manager, String cursor, IterationArguments arguments) {
      // Acquire next iteration result with the blocking manager and handle result on the event loop.
      CompletionStage<IterableIterationResult> cs = acquireNext(handler.getBlockingManager(), manager, cursor, arguments.getCount());
      return cs.thenAcceptAsync(result -> handleIterationResult(result, handler, manager, cursor), ctx.executor())
            .thenApply(ignore -> handler);
   }

   private void handleIterationResult(IterableIterationResult result, Resp3Handler handler, IterationManager manager, String cursor) {
      IterableIterationResult.Status status = result.getStatusCode();
      if (status == IterableIterationResult.Status.InvalidIteration) {
         // Let's just return 0
         emptyIterationResponse(handler);
      } else {
         if (!writeCursor()) {
            Resp3Response.array(writeResponse(result.getEntries()), handler.allocator(), Resp3Type.BULK_STRING);
            return;
         }

         String replyCursor;
         if (status == IterableIterationResult.Status.Finished) {
            // We've reached the end of iteration, return a 0 cursor
            replyCursor = INITIAL_CURSOR;
            manager.close(cursor);
         } else {
            replyCursor = cursor;
         }
         // Array mixes bulk string and arrays.
         Resp3Response.write(handler.allocator(), (res, alloc) -> {
            ByteBufferUtils.writeNumericPrefix(RespConstants.ARRAY, 2, alloc);
            Resp3Response.string(replyCursor, alloc);
            Resp3Response.array(writeResponse(result.getEntries()), alloc, Resp3Type.BULK_STRING);
         });
      }
   }

   private CompletionStage<IterableIterationResult> acquireNext(BlockingManager bm, IterationManager manager, String cursor, int count) {
      return bm.supplyBlocking(() -> manager.next(cursor, count), "resp-iter-" + cursor);
   }

   private void emptyIterationResponse(Resp3Handler handler) {
      // Array mixes a bulk string at first position and an array.
      Resp3Response.write(handler.allocator(), (res, alloc) -> {
         ByteBufferUtils.writeNumericPrefix(RespConstants.ARRAY, 2, alloc);
         Resp3Response.string(INITIAL_CURSOR, alloc);
         Resp3Response.arrayEmpty(alloc);
      });
   }

   protected boolean writeCursor() {
      return true;
   }

   protected abstract IterationManager retrieveIterationManager(Resp3Handler handler);

   protected CompletionStage<IterationInitializationContext> initializeIteration(Resp3Handler handler, List<byte[]> arguments) {
      return null;
   }

   protected abstract String cursor(List<byte[]> raw);

   protected abstract Collection<byte[]> writeResponse(List<CacheEntry> response);
}
