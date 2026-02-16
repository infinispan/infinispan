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
 * CF.ADDNX key item
 * <p>
 * Adds an item to a Cuckoo filter only if it doesn't already exist.
 *
 * @see <a href="https://redis.io/commands/cf.addnx/">CF.ADDNX</a>
 * @since 16.2
 */
public class CFADDNX extends RespCommand implements Resp3Command {

   public CFADDNX() {
      super("CF.ADDNX", 3, 1, 1, 1,
            AclCategory.CUCKOO.mask() | AclCategory.WRITE.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      byte[] item = arguments.get(1);

      FunctionalMap.ReadWriteMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadWriteMap();

      CuckooFilterInsertFunction function = new CuckooFilterInsertFunction(
            Collections.singletonList(item), CuckooFilter.DEFAULT_CAPACITY, false, true);
      CompletionStage<List<Integer>> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (r, w) -> w.integers(r.get(0).longValue()));
   }
}
