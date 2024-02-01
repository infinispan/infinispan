package org.infinispan.server.resp.commands.iteration;

import static org.infinispan.server.resp.RespConstants.CRLF_STRING;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.reactive.publisher.impl.DeliveryGuarantee;
import org.infinispan.server.iteration.IterableIterationResult;
import org.infinispan.server.iteration.IterationInitializationContext;
import org.infinispan.server.iteration.IterationManager;
import org.infinispan.server.iteration.IterationState;
import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

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

      iterate(handler, manager, cursor, args);
      return handler.myStage();
   }

   private CompletionStage<RespRequestHandler> initializeAndIterate(Resp3Handler handler, ChannelHandlerContext ctx,
                                                                    IterationManager manager, IterationArguments arguments,
                                                                    IterationInitializationContext iic) {
      AdvancedCache<Object, Object> cache = handler.cache().withMediaType(MediaType.APPLICATION_OCTET_STREAM, null);
      IterationState iterationState = manager.start(cache, null, arguments.getFilterConverterFactory(),
            arguments.getFilterConverterParams(), null, arguments.getCount(),
            false, DeliveryGuarantee.AT_LEAST_ONCE, iic);
      iterationState.getReaper().registerChannel(ctx.channel());

      if (!ctx.executor().inEventLoop()) {
         return CompletableFuture.supplyAsync(() -> {
            iterate(handler, manager, iterationState.getId(), arguments);
            return handler;
         }, ctx.executor());
      }

      iterate(handler, manager, iterationState.getId(), arguments);
      return handler.myStage();
   }

   private void iterate(Resp3Handler handler, IterationManager manager, String cursor,
                                                       IterationArguments arguments) {
      if (manager == null) {
         ByteBufferUtils.stringToByteBufAscii("*2\r\n$1\r\n0\r\n*0\r\n", handler.allocator());
         return;
      }

      IterableIterationResult result = manager.next(cursor, arguments.getCount());
      IterableIterationResult.Status status = result.getStatusCode();
      if (status == IterableIterationResult.Status.InvalidIteration) {
         // Let's just return 0
         ByteBufferUtils.stringToByteBufAscii("*2\r\n$1\r\n0\r\n*0\r\n", handler.allocator());
      } else {
         if (writeCursor()) {
            StringBuilder response = new StringBuilder();
            response.append("*2\r\n");
            if (status == IterableIterationResult.Status.Finished) {
               // We've reached the end of iteration, return a 0 cursor
               response.append("$1\r\n0\r\n");
               manager.close(cursor);
            } else {
               response.append('$');
               response.append(cursor.length());
               response.append(CRLF_STRING);
               response.append(cursor);
               response.append(CRLF_STRING);
            }
            ByteBufferUtils.stringToByteBufAscii(response, handler.allocator());
         }
         ByteBufferUtils.bytesToResult(writeResponse(result.getEntries()), handler.allocator());
      }
      return;
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
