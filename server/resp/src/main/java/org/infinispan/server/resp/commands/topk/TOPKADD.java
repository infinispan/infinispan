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
 * TOPK.ADD key item [item ...]
 * <p>
 * Adds one or more items to a Top-K filter.
 *
 * @see <a href="https://redis.io/commands/topk.add/">TOPK.ADD</a>
 * @since 16.2
 */
public class TOPKADD extends RespCommand implements Resp3Command {

   public TOPKADD() {
      super("TOPK.ADD", -3, 1, 1, 1,
            AclCategory.TOPK.mask() | AclCategory.WRITE.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {

      byte[] key = arguments.get(0);
      List<byte[]> items = new ArrayList<>(arguments.subList(1, arguments.size()));

      FunctionalMap.ReadWriteMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadWriteMap();

      TopKAddFunction function = new TopKAddFunction(items);
      CompletionStage<List<String>> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (expelled, w) -> {
         w.arrayStart(expelled.size());
         for (String exp : expelled) {
            if (exp == null) {
               w.nulls();
            } else {
               w.simpleString(exp);
            }
         }
         w.arrayEnd();
      });
   }
}
