package org.infinispan.server.resp.commands.bloom;

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
 * BF.ADD key item
 * <p>
 * Adds an item to a Bloom filter. Creates the filter if it doesn't exist.
 *
 * @see <a href="https://redis.io/commands/bf.add/">BF.ADD</a>
 * @since 16.2
 */
public class BFADD extends RespCommand implements Resp3Command {

   public BFADD() {
      super("BF.ADD", 3, 1, 1, 1,
            AclCategory.BLOOM.mask() | AclCategory.WRITE.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      byte[] item = arguments.get(1);

      FunctionalMap.ReadWriteMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadWriteMap();

      BloomFilterInsertFunction function = new BloomFilterInsertFunction(
            Collections.singletonList(item),
            BloomFilter.DEFAULT_CAPACITY, BloomFilter.DEFAULT_ERROR_RATE,
            BloomFilter.DEFAULT_EXPANSION, false, false);
      CompletionStage<List<Integer>> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (r, w) -> w.integers(r.get(0).longValue()));
   }
}
