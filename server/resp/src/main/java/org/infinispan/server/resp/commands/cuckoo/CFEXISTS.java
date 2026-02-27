package org.infinispan.server.resp.commands.cuckoo;

import java.util.Collections;
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
 * CF.EXISTS key item
 * <p>
 * Determines whether an item was added to a Cuckoo filter.
 *
 * @see <a href="https://redis.io/commands/cf.exists/">CF.EXISTS</a>
 * @since 16.2
 */
public class CFEXISTS extends RespCommand implements Resp3Command {

   public CFEXISTS() {
      super("CF.EXISTS", 3, 1, 1, 1,
            AclCategory.CUCKOO.mask() | AclCategory.READ.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      byte[] item = arguments.get(1);

      FunctionalMap.ReadOnlyMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadOnlyMap();

      CuckooFilterExistsFunction function = new CuckooFilterExistsFunction(Collections.singletonList(item));
      CompletionStage<List<Integer>> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (r, w) -> w.integers(r.get(0).longValue()));
   }
}
