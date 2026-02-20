package org.infinispan.server.resp.commands.bitmap;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * BITPOS
 *
 * @see <a href="https://redis.io/commands/bitpos/">BITPOS</a>
 * @since 16.2
 */
public class BITPOS extends RespCommand implements Resp3Command {
   public BITPOS() {
      super(-3, 1, 1, 1, AclCategory.READ.mask() | AclCategory.BITMAP.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      int bit = Integer.parseInt(new String(arguments.get(1)));

      CompletableFuture<byte[]> async = handler.cache().getAsync(key);
      return handler.stageToReturn(async.thenApply(value -> {
         if (value == null) {
            return (long) (bit == 0 ? 0 : -1);
         }

         int start = 0;
         int end = value.length - 1;

         if (arguments.size() > 2) {
            start = Integer.parseInt(new String(arguments.get(2)));
            if (start < 0) {
               start = value.length + start;
            }
         }

         if (arguments.size() > 3) {
            end = Integer.parseInt(new String(arguments.get(3)));
            if (end < 0) {
               end = value.length + end;
            }
         }
         start = Math.max(0, start);
         end = Math.min(value.length - 1, end);

         for (int i = start; i <= end; i++) {
            byte b = value[i];
            if (bit == 0) {
               b = (byte) ~b;
            }
            if (b != 0) {
               for (int j = 0; j < 8; j++) {
                  if (((b >> (7 - j)) & 1) == 1) {
                     return i * 8L + j;
                  }
               }
            }
         }

         return bit == 0 && (end + 1) * 8 < value.length * 8 ? (end + 1) * 8L : -1;
      }), ctx, (v, ch) -> ch.integers(v));
   }
}
