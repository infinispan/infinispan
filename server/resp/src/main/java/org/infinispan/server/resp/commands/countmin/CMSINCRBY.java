package org.infinispan.server.resp.commands.countmin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.ProbabilisticErrors;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * CMS.INCRBY key item increment [item increment ...]
 * <p>
 * Increases the count of one or more items by increment.
 *
 * @see <a href="https://redis.io/commands/cms.incrby/">CMS.INCRBY</a>
 * @since 16.2
 */
public class CMSINCRBY extends RespCommand implements Resp3Command {

   public CMSINCRBY() {
      // No @slow: matches COMMAND INFO output, despite docs claiming @slow
      super("CMS.INCRBY", -4, 1, 1, 1,
            AclCategory.CMS.mask() | AclCategory.WRITE.mask());
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
            long value = ArgumentUtils.toLong(arguments.get(i + 1));
            if (value < 0) {
               handler.writer().customError(ProbabilisticErrors.CMS_NEGATIVE_NUMBER);
               return handler.myStage();
            }
            increments.add(value);
         } catch (NumberFormatException e) {
            handler.writer().customError(ProbabilisticErrors.CMS_CANNOT_PARSE);
            return handler.myStage();
         }
      }

      FunctionalMap.ReadWriteMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadWriteMap();

      CmsIncrByFunction function = new CmsIncrByFunction(items, increments);
      CompletionStage<List<Long>> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, ResponseWriter.ARRAY_INTEGER);
   }
}
