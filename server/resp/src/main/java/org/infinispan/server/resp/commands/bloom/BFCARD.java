package org.infinispan.server.resp.commands.bloom;

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
 * BF.CARD key
 * <p>
 * Returns the cardinality of a Bloom filter (number of items added).
 *
 * @see <a href="https://redis.io/commands/bf.card/">BF.CARD</a>
 * @since 16.2
 */
public class BFCARD extends RespCommand implements Resp3Command {

   public BFCARD() {
      super("BF.CARD", 2, 1, 1, 1,
            AclCategory.BLOOM.mask() | AclCategory.READ.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);

      FunctionalMap.ReadOnlyMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadOnlyMap();

      CompletionStage<Long> result = cache.eval(key, BloomFilterCardFunction.INSTANCE);

      return handler.stageToReturn(result, ctx, (r, w) -> w.integers(r));
   }
}
