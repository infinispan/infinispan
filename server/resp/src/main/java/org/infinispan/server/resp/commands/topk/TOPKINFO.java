package org.infinispan.server.resp.commands.topk;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.topk.TopKInfoFunction.TopKInfo;

import io.netty.channel.ChannelHandlerContext;

/**
 * TOPK.INFO key
 * <p>
 * Returns information about a Top-K filter.
 *
 * @see <a href="https://redis.io/commands/topk.info/">TOPK.INFO</a>
 * @since 16.2
 */
public class TOPKINFO extends RespCommand implements Resp3Command {

   public TOPKINFO() {
      super("TOPK.INFO", 2, 1, 1, 1,
            AclCategory.TOPK.mask() | AclCategory.READ.mask() | AclCategory.FAST.mask());
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

      CompletionStage<TopKInfo> result = cache.eval(key, TopKInfoFunction.INSTANCE);

      return handler.stageToReturn(result, ctx, (info, w) -> {
         Map<String, Object> map = info.toMap();
         w.arrayStart(map.size() * 2);
         for (Map.Entry<String, Object> entry : map.entrySet()) {
            w.simpleString(entry.getKey());
            Object value = entry.getValue();
            if (value instanceof Long) {
               w.integers((Long) value);
            } else if (value instanceof Double) {
               w.doubles((Double) value);
            } else {
               w.simpleString(value.toString());
            }
         }
         w.arrayEnd();
      });
   }
}
