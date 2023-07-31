package org.infinispan.server.resp.commands.iteration;

import static org.infinispan.server.resp.RespConstants.CRLF_STRING;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletionStage;

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

   @Override
   public final CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      IterationArguments args = IterationArguments.parse(handler, arguments);
      if (args == null) return handler.myStage();

      IterationManager manager = retrieveIterationManager(handler);
      String cursor = cursor(arguments);

      if (INITIAL_CURSOR.equals(cursor)) {
         CompletionStage<IterationInitializationContext> initialization = initializeIteration(handler, arguments);
         if (initialization != null) {
            return initialization.thenCompose(iic -> initializeAndIterate(handler, ctx, manager, args, iic));
         }
         return initializeAndIterate(handler, ctx, manager, args, null);
      }

      return iterate(handler, manager, cursor, args);
   }

   private CompletionStage<RespRequestHandler> initializeAndIterate(Resp3Handler handler, ChannelHandlerContext ctx,
                                                                    IterationManager manager, IterationArguments arguments,
                                                                    IterationInitializationContext iic) {
      IterationState iterationState = manager.start(handler.cache(), null, arguments.getFilterConverterFactory(),
            arguments.getFilterConverterParams(), MediaType.APPLICATION_OCTET_STREAM, arguments.getCount(),
            false, DeliveryGuarantee.AT_LEAST_ONCE, iic);
      iterationState.getReaper().registerChannel(ctx.channel());
      return iterate(handler, manager, iterationState.getId(), arguments);
   }

   private CompletionStage<RespRequestHandler> iterate(Resp3Handler handler, IterationManager manager, String cursor,
                                                       IterationArguments arguments) {
      if (manager == null) {
         ByteBufferUtils.stringToByteBufAscii("*2\r\n$1\r\n0\r\n*0\r\n", handler.allocator());
         return handler.myStage();
      }

      IterableIterationResult result = manager.next(cursor, arguments.getCount());
      IterableIterationResult.Status status = result.getStatusCode();
      if (status == IterableIterationResult.Status.InvalidIteration) {
         // Let's just return 0
         ByteBufferUtils.stringToByteBufAscii("*2\r\n$1\r\n0\r\n*0\r\n", handler.allocator());
      } else {
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
         ByteBufferUtils.bytesToResult(writeResponse(result.getEntries()), handler.allocator());
      }
      return handler.myStage();
   }

   protected abstract IterationManager retrieveIterationManager(Resp3Handler handler);

   protected CompletionStage<IterationInitializationContext> initializeIteration(Resp3Handler handler, List<byte[]> arguments) {
      return null;
   }

   protected abstract String cursor(List<byte[]> raw);

   protected abstract Collection<byte[]> writeResponse(List<CacheEntry> response);
}
