package org.infinispan.server.resp.commands.bitmap;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * BITCOUNT
 *
 * @see <a href="https://redis.io/commands/bitcount/">BITCOUNT</a>
 * @since 16.2
 */
public class BITCOUNT extends RespCommand implements Resp3Command {
   public BITCOUNT() {
      super(-2, 1, 1, 1, AclCategory.READ.mask() | AclCategory.BITMAP.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);

      CompletableFuture<byte[]> async = handler.cache().getAsync(key);
      return handler.stageToReturn(async.thenApply(value -> {
         if (value == null) {
            return 0L;
         }
         if (arguments.size() > 1) {
            int start = ArgumentUtils.toInt(arguments.get(1));
            int end = ArgumentUtils.toInt(arguments.get(2));
            if (start < 0) {
               start = value.length + start;
            }
            if (end < 0) {
               end = value.length + end;
            }
            start = Math.max(0, start);
            end = Math.min(value.length - 1, end);

            long count = 0;
            for (int i = start; i <= end; i++) {
               count += Integer.bitCount(value[i] & 0xFF);
            }
            return count;
         }

         long count = 0;
         for (byte b : value) {
            count += Integer.bitCount(b & 0xFF);
         }
         return count;
      }), ctx, (v, ch) -> ch.integers(v));
   }
}
