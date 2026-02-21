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
 * TOPK.COUNT key item [item ...]
 * <p>
 * Returns count for one or more items in a Top-K filter.
 * <p>
 * Note: This command is deprecated in Redis but still supported.
 *
 * @see <a href="https://redis.io/commands/topk.count/">TOPK.COUNT</a>
 * @since 16.2
 */
public class TOPKCOUNT extends RespCommand implements Resp3Command {

   public TOPKCOUNT() {
      super("TOPK.COUNT", -3, 1, 1, 1,
            AclCategory.TOPK.mask() | AclCategory.READ.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      List<byte[]> items = new ArrayList<>(arguments.subList(1, arguments.size()));

      FunctionalMap.ReadOnlyMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadOnlyMap();

      TopKCountFunction function = new TopKCountFunction(items);
      CompletionStage<List<Long>> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (counts, w) -> {
         w.arrayStart(counts.size());
         for (Long count : counts) {
            w.integers(count);
         }
         w.arrayEnd();
      });
   }
}
