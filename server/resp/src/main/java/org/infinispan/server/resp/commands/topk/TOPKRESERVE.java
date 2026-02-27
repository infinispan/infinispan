package org.infinispan.server.resp.commands.topk;

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
      byte[] key = arguments.get(0);
      int k;
      int width = TopK.DEFAULT_WIDTH;
      int depth = TopK.DEFAULT_DEPTH;
      double decay = TopK.DEFAULT_DECAY;

      try {
         k = Integer.parseInt(new String(arguments.get(1)));
         if (arguments.size() > 2) {
            width = Integer.parseInt(new String(arguments.get(2)));
         }
         if (arguments.size() > 3) {
            depth = Integer.parseInt(new String(arguments.get(3)));
         }
         if (arguments.size() > 4) {
            decay = Double.parseDouble(new String(arguments.get(4)));
         }
      } catch (NumberFormatException e) {
         handler.writer().customError("ERR invalid parameter value");
         return handler.myStage();
      }

      if (k <= 0) {
         handler.writer().customError("ERR k must be positive");
         return handler.myStage();
      }

      if (width <= 0) {
         handler.writer().customError("ERR width must be positive");
         return handler.myStage();
      }

      if (depth <= 0) {
         handler.writer().customError("ERR depth must be positive");
         return handler.myStage();
      }

      if (decay <= 0 || decay > 1) {
         handler.writer().customError("ERR decay must be between 0 (exclusive) and 1 (inclusive)");
         return handler.myStage();
      }

      FunctionalMap.ReadWriteMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadWriteMap();

      TopKReserveFunction function = new TopKReserveFunction(k, width, depth, decay);
      CompletionStage<Boolean> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (r, w) -> w.ok());
   }
}
