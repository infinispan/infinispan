package org.infinispan.server.resp.commands.string;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * GETRANGE Resp Command
 *
 * Returns the substring of the string value stored at key, determined by the
 * offsets start and end (both are inclusive).
 * Negative offsets can be used in order to provide an offset starting from the
 * end of the string.
 * So -1 means the last character, -2 the penultimate and so forth.
 *
 * The function handles out of range requests by limiting the resulting range to
 * the actual length of the string.
 *
 * @link https://redis.io/commands/getrange/
 * @since 15.0
 */
public class GETRANGE extends RespCommand implements Resp3Command {
   private static byte[] EMPTYBYTES = new byte[0];
   public GETRANGE() {
      super(4, 1, 1, 1);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      byte[] keyBytes = arguments.get(0);
      int beginIndex = ArgumentUtils.toInt(arguments.get(1));
      int lastIndex =  ArgumentUtils.toInt(arguments.get(2));
      CompletionStage<byte[]> objectCompletableFuture = handler.cache().getAsync(keyBytes)
            .thenApply(value -> subrange(value, beginIndex, lastIndex));
      return handler.stageToReturn(objectCompletableFuture, ctx, Consumers.BULK_BICONSUMER);
   }

   private byte[] subrange(byte[] arr, int begin, int end) {
      // Deal with negative
      if (begin < 0) {
         begin = Math.max(0, arr.length + begin);
      }
      if (end < 0) {
         end = arr.length + end;
      } else {
         end = Math.min(arr.length, end);
      }
      // Quick return for oo range
      if (begin >= end || begin >= arr.length || end <= 0) {
         return EMPTYBYTES;
      }
      return Arrays.copyOfRange(arr, begin, end+1);
   }
}
