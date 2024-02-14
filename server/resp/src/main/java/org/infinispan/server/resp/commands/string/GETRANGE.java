package org.infinispan.server.resp.commands.string;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.Util;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

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
      if (arr == null) return Util.EMPTY_BYTE_ARRAY;

      // Deal with negative
      if (begin < 0) {
         begin = Math.max(0, arr.length + begin);
      }
      if (end < 0) {
         end = arr.length + end;
      } else {
         end = Math.min(arr.length - 1, end);
      }
      // Quick return for oo range
      if (begin >= end || begin >= arr.length) {
         return Util.EMPTY_BYTE_ARRAY;
      }
      return Arrays.copyOfRange(arr, begin, end + 1);
   }
}
