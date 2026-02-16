package org.infinispan.server.resp.commands.tdigest;

import java.util.ArrayList;
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
 * TDIGEST.ADD key value [value ...]
 * <p>
 * Adds one or more values to a t-digest sketch.
 *
 * @see <a href="https://redis.io/commands/tdigest.add/">TDIGEST.ADD</a>
 * @since 16.2
 */
public class TDIGESTADD extends RespCommand implements Resp3Command {

   public TDIGESTADD() {
      super("TDIGEST.ADD", -3, 1, 1, 1,
            AclCategory.TDIGEST.mask() | AclCategory.WRITE.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      List<Double> values = new ArrayList<>();

      for (int i = 1; i < arguments.size(); i++) {
         try {
            values.add(Double.parseDouble(new String(arguments.get(i))));
         } catch (NumberFormatException e) {
            handler.writer().customError("ERR invalid value");
            return handler.myStage();
         }
      }

      FunctionalMap.ReadWriteMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadWriteMap();

      TDigestAddFunction function = new TDigestAddFunction(values);
      CompletionStage<Boolean> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (r, w) -> w.ok());
   }
}
