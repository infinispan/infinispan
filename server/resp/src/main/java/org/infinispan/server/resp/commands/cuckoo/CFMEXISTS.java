package org.infinispan.server.resp.commands.cuckoo;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Type;

import io.netty.channel.ChannelHandlerContext;

/**
 * CF.MEXISTS key item [item ...]
 * <p>
 * Determines whether one or more items were added to a Cuckoo filter.
 *
 * @see <a href="https://redis.io/commands/cf.mexists/">CF.MEXISTS</a>
 * @since 16.2
 */
public class CFMEXISTS extends RespCommand implements Resp3Command {

   public CFMEXISTS() {
      super("CF.MEXISTS", -3, 1, 1, 1,
            AclCategory.CUCKOO.mask() | AclCategory.READ.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      List<byte[]> items = new ArrayList<>(arguments.subList(1, arguments.size()));

      FunctionalMap.ReadOnlyMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadOnlyMap();

      CuckooFilterExistsFunction function = new CuckooFilterExistsFunction(items);
      CompletionStage<List<Integer>> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (r, w) ->
            w.array(r, Resp3Type.INTEGER));
   }
}
