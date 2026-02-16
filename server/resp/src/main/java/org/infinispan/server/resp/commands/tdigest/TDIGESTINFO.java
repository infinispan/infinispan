package org.infinispan.server.resp.commands.tdigest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.tdigest.TDigestInfoFunction.TDigestInfo;

import io.netty.channel.ChannelHandlerContext;

/**
 * TDIGEST.INFO key
 * <p>
 * Returns information about a t-digest.
 *
 * @see <a href="https://redis.io/commands/tdigest.info/">TDIGEST.INFO</a>
 * @since 16.2
 */
public class TDIGESTINFO extends RespCommand implements Resp3Command {

   public TDIGESTINFO() {
      super("TDIGEST.INFO", 2, 1, 1, 1,
            AclCategory.TDIGEST.mask() | AclCategory.READ.mask() | AclCategory.FAST.mask());
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

      CompletionStage<TDigestInfo> result = cache.eval(key, TDigestInfoFunction.INSTANCE);

      return handler.stageToReturn(result, ctx, (info, w) -> {
         Map<String, Long> map = info.toMap();
         w.arrayStart(map.size() * 2);
         for (Map.Entry<String, Long> entry : map.entrySet()) {
            w.simpleString(entry.getKey());
            w.integers(entry.getValue());
         }
         w.arrayEnd();
      });
   }
}
