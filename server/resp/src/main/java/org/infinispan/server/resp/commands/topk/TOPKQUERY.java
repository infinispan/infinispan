package org.infinispan.server.resp.commands.topk;

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
 * TOPK.QUERY key item [item ...]
 * <p>
 * Checks whether one or more items are in the Top-K.
 *
 * @see <a href="https://redis.io/commands/topk.query/">TOPK.QUERY</a>
 * @since 16.2
 */
public class TOPKQUERY extends RespCommand implements Resp3Command {

   public TOPKQUERY() {
      super("TOPK.QUERY", -3, 1, 1, 1,
            AclCategory.TOPK.mask() | AclCategory.READ.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      List<byte[]> items = new ArrayList<>(arguments.subList(1, arguments.size()));

      FunctionalMap.ReadOnlyMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadOnlyMap();

      TopKQueryFunction function = new TopKQueryFunction(items);
      CompletionStage<List<Long>> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (results, w) -> {
         w.arrayStart(results.size());
         for (Long r : results) {
            w.integers(r);
         }
         w.arrayEnd();
      });
   }
}
