package org.infinispan.server.resp.commands.tdigest;

import java.util.ArrayList;
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
 * TDIGEST.RANK key value [value ...]
 * <p>
 * Returns the rank of one or more values.
 *
 * @see <a href="https://redis.io/commands/tdigest.rank/">TDIGEST.RANK</a>
 * @since 16.2
 */
public class TDIGESTRANK extends RespCommand implements Resp3Command {

   public TDIGESTRANK() {
      super("TDIGEST.RANK", -3, 1, 1, 1,
            AclCategory.TDIGEST.mask() | AclCategory.READ.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      if (arguments.size() < 2) {
         handler.writer().wrongArgumentNumber(this);
         return handler.myStage();
      }

      byte[] key = arguments.get(0);
      List<Double> values = new ArrayList<>();

      for (int i = 1; i < arguments.size(); i++) {
         try {
            values.add(Double.parseDouble(new String(arguments.get(i))));
         } catch (NumberFormatException e) {
            handler.writer().customError("ERR invalid value");
            return handler.myStage();
         }
      }

      FunctionalMap.ReadOnlyMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadOnlyMap();

      TDigestRankFunction function = new TDigestRankFunction(values);
      CompletionStage<List<Long>> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (ranks, w) -> {
         w.arrayStart(ranks.size());
         for (Long rank : ranks) {
            w.integers(rank);
         }
         w.arrayEnd();
      });
   }
}
