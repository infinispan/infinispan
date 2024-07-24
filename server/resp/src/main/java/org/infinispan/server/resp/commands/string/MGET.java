package org.infinispan.server.resp.commands.string;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

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
         ByteBufferUtils.stringToByteBufAscii("*0\r\n", handler.allocator());
         return handler.myStage();
      }
      AtomicInteger resultBytesSize = new AtomicInteger();
      CompletionStage<List<byte[]>> results = CompletionStages.performSequentially(arguments.iterator(), k -> getAsync(handler, k)
            .exceptionally(MGET::handleWrongTypeError)
            .handle((returnValue, t) -> {
               if (returnValue != null) {
                  int length = returnValue.length;
                  if (length > 0) {
                     // $ + digit length (log10 + 1) + /r/n + byte length
                     resultBytesSize.addAndGet(1 + (int) Math.log10(length) + 1 + 2 + returnValue.length);
                  } else {
                     // $0 + /r/n
                     resultBytesSize.addAndGet(2 + 2);
                  }
               } else {
                  // _
                  resultBytesSize.addAndGet(1);
               }
               // /r/n
               resultBytesSize.addAndGet(2);
               return returnValue;
            }), Collectors.toList());
      return handler.stageToReturn(results, ctx, (r, alloc) -> ByteBufferUtils.bytesToResult(resultBytesSize.get(), r, alloc));
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
