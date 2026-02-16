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
 * TDIGEST.QUANTILE key quantile [quantile ...]
 * <p>
 * Returns an estimation of the value at the given quantile(s).
 *
 * @see <a href="https://redis.io/commands/tdigest.quantile/">TDIGEST.QUANTILE</a>
 * @since 16.2
 */
public class TDIGESTQUANTILE extends RespCommand implements Resp3Command {

   public TDIGESTQUANTILE() {
      super("TDIGEST.QUANTILE", -3, 1, 1, 1,
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
      List<Double> fractions = new ArrayList<>();

      for (int i = 1; i < arguments.size(); i++) {
         try {
            double fraction = Double.parseDouble(new String(arguments.get(i)));
            fractions.add(fraction);
         } catch (NumberFormatException e) {
            handler.writer().customError("ERR invalid quantile value");
            return handler.myStage();
         }
      }

      FunctionalMap.ReadOnlyMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadOnlyMap();

      TDigestQuantileFunction function = new TDigestQuantileFunction(fractions);
      CompletionStage<List<Double>> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (values, w) -> {
         w.arrayStart(values.size());
         for (Double value : values) {
            w.doubles(value);
         }
         w.arrayEnd();
      });
   }
}
