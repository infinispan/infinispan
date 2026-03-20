package org.infinispan.server.resp.commands.bloom;

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
 * BF.MEXISTS key item [item ...]
 * <p>
 * Determines whether one or more items were added to a Bloom filter.
 *
 * @see <a href="https://redis.io/commands/bf.mexists/">BF.MEXISTS</a>
 * @since 16.2
 */
public class BFMEXISTS extends RespCommand implements Resp3Command {

   public BFMEXISTS() {
      super("BF.MEXISTS", -3, 1, 1, 1,
            AclCategory.BLOOM.mask() | AclCategory.READ.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      List<byte[]> items = new ArrayList<>(arguments.subList(1, arguments.size()));

      FunctionalMap.ReadOnlyMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadOnlyMap();

      BloomFilterExistsFunction function = new BloomFilterExistsFunction(items);
      CompletionStage<List<Integer>> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (r, w) ->
            w.array(r, Resp3Type.INTEGER));
   }
}
