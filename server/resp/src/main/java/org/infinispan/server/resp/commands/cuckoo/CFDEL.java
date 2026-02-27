package org.infinispan.server.resp.commands.cuckoo;

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
 * CF.DEL key item
 * <p>
 * Deletes an item from a Cuckoo filter.
 *
 * @see <a href="https://redis.io/commands/cf.del/">CF.DEL</a>
 * @since 16.2
 */
public class CFDEL extends RespCommand implements Resp3Command {

   public CFDEL() {
      super("CF.DEL", 3, 1, 1, 1,
            AclCategory.CUCKOO.mask() | AclCategory.WRITE.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      byte[] item = arguments.get(1);

      FunctionalMap.ReadWriteMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadWriteMap();

      CuckooFilterDelFunction function = new CuckooFilterDelFunction(item);
      CompletionStage<Integer> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (r, w) -> w.integers(r.longValue()));
   }
}
