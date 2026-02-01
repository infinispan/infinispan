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
 * GETBIT
 *
 * @see <a href="https://redis.io/commands/getbit/">GETBIT</a>
 * @since 16.2
 */
public class GETBIT extends RespCommand implements Resp3Command {
   public GETBIT() {
      super(3, 1, 1, 1, AclCategory.READ.mask() | AclCategory.STRING.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,

                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      int offset = Integer.parseInt(new String(arguments.get(1)));

      CompletableFuture<byte[]> async = handler.cache().getAsync(key);
      return handler.stageToReturn(async.thenApply(value -> {
         if (value == null) {
            return 0L;
         }

         int byteIndex = offset / 8;
         if (byteIndex >= value.length) {
            return 0L;
         }

         int bitIndex = offset % 8;
         return (value[byteIndex] >> (7 - bitIndex)) & 1;
      }), ctx, (v, ch) -> ch.integers(v.longValue()));
   }
}
