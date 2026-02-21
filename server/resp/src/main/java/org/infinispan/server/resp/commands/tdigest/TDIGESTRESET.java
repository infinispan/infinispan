package org.infinispan.server.resp.commands.tdigest;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * TDIGEST.RESET key
 * <p>
 * Resets a t-digest sketch, emptying it.
 *
 * @see <a href="https://redis.io/commands/tdigest.reset/">TDIGEST.RESET</a>
 * @since 16.2
 */
public class TDIGESTRESET extends RespCommand implements Resp3Command {

   public TDIGESTRESET() {
      super("TDIGEST.RESET", 2, 1, 1, 1,
            AclCategory.TDIGEST.mask() | AclCategory.WRITE.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      if (arguments.size() != 1) {
         handler.writer().wrongArgumentNumber(this);
         return handler.myStage();
      }

      byte[] key = arguments.get(0);

      FunctionalMap.ReadWriteMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadWriteMap();

      CompletionStage<Boolean> result = cache.eval(key, TDigestResetFunction.INSTANCE);

      return handler.stageToReturn(result, ctx, (r, w) -> w.ok());
   }
}
