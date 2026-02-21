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
 * TDIGEST.MIN key
 * <p>
 * Returns the minimum value from a t-digest.
 *
 * @see <a href="https://redis.io/commands/tdigest.min/">TDIGEST.MIN</a>
 * @since 16.2
 */
public class TDIGESTMIN extends RespCommand implements Resp3Command {

   public TDIGESTMIN() {
      super("TDIGEST.MIN", 2, 1, 1, 1,
            AclCategory.TDIGEST.mask() | AclCategory.READ.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      if (arguments.size() != 1) {
         handler.writer().wrongArgumentNumber(this);
         return handler.myStage();
      }

      byte[] key = arguments.get(0);

      FunctionalMap.ReadOnlyMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadOnlyMap();

      CompletionStage<Double> result = cache.eval(key, TDigestMinFunction.INSTANCE);

      return handler.stageToReturn(result, ctx, (value, w) -> w.doubles(value));
   }
}
