package org.infinispan.server.resp.commands.topk;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.ProbabilisticErrors;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * TOPK.LIST key [WITHCOUNT]
 * <p>
 * Returns full list of items in Top-K filter.
 *
 * @see <a href="https://redis.io/commands/topk.list/">TOPK.LIST</a>
 * @since 16.2
 */
public class TOPKLIST extends RespCommand implements Resp3Command {

   private static final byte[] WITHCOUNT = "WITHCOUNT".getBytes(StandardCharsets.US_ASCII);

   public TOPKLIST() {
      super("TOPK.LIST", -2, 1, 1, 1,
            AclCategory.TOPK.mask() | AclCategory.READ.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      boolean withCount = false;

      if (arguments.size() == 2) {
         if (RespUtil.isAsciiBytesEquals(WITHCOUNT, arguments.get(1))) {
            withCount = true;
         } else {
            handler.writer().customError(ProbabilisticErrors.TOPK_WITHCOUNT_EXPECTED);
            return handler.myStage();
         }
      }

      FunctionalMap.ReadOnlyMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadOnlyMap();

      TopKListFunction function = new TopKListFunction(withCount);
      CompletionStage<List<Object>> result = cache.eval(key, function);

      boolean finalWithCount = withCount;
      return handler.stageToReturn(result, ctx, (items, w) -> {
         w.arrayStart(items.size());
         for (int i = 0; i < items.size(); i++) {
            Object item = items.get(i);
            if (finalWithCount && i % 2 == 1) {
               w.integers((Long) item);
            } else {
               w.string((CharSequence) item);
            }
         }
         w.arrayEnd();
      });
   }
}
