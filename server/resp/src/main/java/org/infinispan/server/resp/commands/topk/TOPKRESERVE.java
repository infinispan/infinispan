package org.infinispan.server.resp.commands.topk;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.ProbabilisticErrors;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * TOPK.RESERVE key topk [width depth decay]
 * <p>
 * Initializes a Top-K filter with specified parameters.
 *
 * @see <a href="https://redis.io/commands/topk.reserve/">TOPK.RESERVE</a>
 * @since 16.2
 */
public class TOPKRESERVE extends RespCommand implements Resp3Command {

   public TOPKRESERVE() {
      super("TOPK.RESERVE", -3, 1, 1, 1,
            AclCategory.TOPK.mask() | AclCategory.WRITE.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      // Redis requires exactly 2 or 5 args: key topk [width depth decay]
      if (arguments.size() != 2 && arguments.size() != 5) {
         handler.writer().wrongArgumentNumber(this);
         return handler.myStage();
      }

      byte[] key = arguments.get(0);
      int k;
      int width = TopK.DEFAULT_WIDTH;
      int depth = TopK.DEFAULT_DEPTH;
      double decay = TopK.DEFAULT_DECAY;

      try {
         k = ArgumentUtils.toInt(arguments.get(1));
      } catch (NumberFormatException e) {
         handler.writer().customError(ProbabilisticErrors.TOPK_INVALID_K);
         return handler.myStage();
      }

      if (k <= 0) {
         handler.writer().customError(ProbabilisticErrors.TOPK_INVALID_K);
         return handler.myStage();
      }

      if (arguments.size() == 5) {
         try {
            width = ArgumentUtils.toInt(arguments.get(2));
         } catch (NumberFormatException e) {
            handler.writer().customError(ProbabilisticErrors.TOPK_INVALID_WIDTH);
            return handler.myStage();
         }
         try {
            depth = ArgumentUtils.toInt(arguments.get(3));
         } catch (NumberFormatException e) {
            handler.writer().customError(ProbabilisticErrors.TOPK_INVALID_DEPTH);
            return handler.myStage();
         }
         try {
            decay = ArgumentUtils.toDouble(arguments.get(4));
         } catch (NumberFormatException e) {
            handler.writer().customError(ProbabilisticErrors.TOPK_INVALID_DECAY);
            return handler.myStage();
         }
      }

      if (width <= 0) {
         handler.writer().customError(ProbabilisticErrors.TOPK_INVALID_WIDTH);
         return handler.myStage();
      }

      if (depth <= 0) {
         handler.writer().customError(ProbabilisticErrors.TOPK_INVALID_DEPTH);
         return handler.myStage();
      }

      if (decay <= 0 || decay > 1) {
         handler.writer().customError(ProbabilisticErrors.TOPK_INVALID_DECAY);
         return handler.myStage();
      }

      FunctionalMap.ReadWriteMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadWriteMap();

      TopKReserveFunction function = new TopKReserveFunction(k, width, depth, decay);
      CompletionStage<Boolean> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (r, w) -> w.ok());
   }
}
