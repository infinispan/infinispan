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
 * BF.MADD key item [item ...]
 * <p>
 * Adds one or more items to a Bloom filter. Creates the filter if it doesn't exist.
 *
 * @see <a href="https://redis.io/commands/bf.madd/">BF.MADD</a>
 * @since 16.2
 */
public class BFMADD extends RespCommand implements Resp3Command {

   public BFMADD() {
      super("BF.MADD", -3, 1, 1, 1,
            AclCategory.BLOOM.mask() | AclCategory.WRITE.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      List<byte[]> items = new ArrayList<>(arguments.subList(1, arguments.size()));

      FunctionalMap.ReadWriteMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadWriteMap();

      BloomFilterInsertFunction function = new BloomFilterInsertFunction(
            items,
            BloomFilter.DEFAULT_CAPACITY, BloomFilter.DEFAULT_ERROR_RATE,
            BloomFilter.DEFAULT_EXPANSION, false, false);
      CompletionStage<List<Integer>> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (r, w) -> w.array(r, Resp3Type.INTEGER));
   }
}
