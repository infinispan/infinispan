package org.infinispan.server.resp.commands.string;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Response;

import io.netty.channel.ChannelHandlerContext;

/**
 * @link https://redis.io/commands/mget/
 * @since 14.0
 */
public class MGET extends RespCommand implements Resp3Command {

   private static final Function<Object, byte[]> TYPE_CHECKER = res -> {
      if (!(res instanceof byte[]))
         return null;
      return (byte[]) res;
   };

   public MGET() {
      super(-2, 1, -1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      int keysToRetrieve = arguments.size();
      if (keysToRetrieve == 0) {
         Resp3Response.arrayEmpty(handler.allocator());
         return handler.myStage();
      }
      CompletionStage<List<byte[]>> results = CompletionStages.performSequentially(arguments.iterator(), k -> getAsync(handler, k)
            .exceptionally(MGET::handleWrongTypeError), Collectors.toList());
      return handler.stageToReturn(results, ctx, Resp3Response.ARRAY_BULK_STRING);
   }

   private static CompletionStage<byte[]> getAsync(Resp3Handler handler, byte[] key) {
      CompletableFuture<?> async;
      try {
         async = handler.cache().getAsync(key);
      } catch (Exception ex) {
         async = CompletableFuture.completedFuture(handleWrongTypeError(ex));
      }
      return async.thenApply(TYPE_CHECKER);
   }

   private static byte[] handleWrongTypeError(Throwable ex) {
      if (RespErrorUtil.isWrongTypeError(ex)) {
         return null;
      }
      throw CompletableFutures.asCompletionException(ex);
   }
}
