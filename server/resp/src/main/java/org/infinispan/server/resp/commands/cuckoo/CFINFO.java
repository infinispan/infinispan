package org.infinispan.server.resp.commands.cuckoo;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.cuckoo.CuckooFilterInfoFunction.CuckooFilterInfo;
import org.infinispan.server.resp.serialization.Resp3Type;

import io.netty.channel.ChannelHandlerContext;

/**
 * CF.INFO key
 * <p>
 * Returns information about a Cuckoo filter.
 *
 * @see <a href="https://redis.io/commands/cf.info/">CF.INFO</a>
 * @since 16.2
 */
public class CFINFO extends RespCommand implements Resp3Command {

   public CFINFO() {
      super("CF.INFO", 2, 1, 1, 1,
            AclCategory.CUCKOO.mask() | AclCategory.READ.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);

      FunctionalMap.ReadOnlyMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadOnlyMap();

      CompletionStage<CuckooFilterInfo> result = cache.eval(key, CuckooFilterInfoFunction.INSTANCE);

      return handler.stageToReturn(result, ctx, (info, w) ->
            w.map(info.toMap(), Resp3Type.SIMPLE_STRING, Resp3Type.INTEGER));
   }
}
