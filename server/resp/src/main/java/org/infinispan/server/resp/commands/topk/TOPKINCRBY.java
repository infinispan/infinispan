package org.infinispan.server.resp.commands.topk;

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
 * TOPK.INCRBY key item increment [item increment ...]
 * <p>
 * Increases the count of one or more items by increment.
 *
 * @see <a href="https://redis.io/commands/topk.incrby/">TOPK.INCRBY</a>
 * @since 16.2
 */
public class TOPKINCRBY extends RespCommand implements Resp3Command {

   public TOPKINCRBY() {
      super("TOPK.INCRBY", -4, 1, 1, 1, AclCategory.TOPK.mask() | AclCategory.WRITE.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      if ((arguments.size() - 1) % 2 != 0) {
         handler.writer().wrongArgumentNumber(this);
         return handler.myStage();
      }

      byte[] key = arguments.get(0);
      List<byte[]> items = new ArrayList<>();
      List<Long> increments = new ArrayList<>();

      for (int i = 1; i < arguments.size(); i += 2) {
         items.add(arguments.get(i));
         try {
            increments.add(Long.parseLong(new String(arguments.get(i + 1))));
         } catch (NumberFormatException e) {
            handler.writer().customError("ERR invalid increment value");
            return handler.myStage();
         }
      }

      FunctionalMap.ReadWriteMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadWriteMap();

      TopKIncrByFunction function = new TopKIncrByFunction(items, increments);
      CompletionStage<List<String>> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (expelled, w) -> {
         w.arrayStart(expelled.size());
         for (String exp : expelled) {
            if (exp == null) {
               w.nulls();
            } else {
               w.simpleString(exp);
            }
         }
         w.arrayEnd();
      });
   }
}
