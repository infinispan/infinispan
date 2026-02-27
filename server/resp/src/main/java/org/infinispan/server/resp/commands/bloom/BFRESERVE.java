package org.infinispan.server.resp.commands.bloom;

import static org.infinispan.server.resp.commands.ArgumentUtils.toDouble;
import static org.infinispan.server.resp.commands.ArgumentUtils.toInt;
import static org.infinispan.server.resp.commands.ArgumentUtils.toLong;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * BF.RESERVE key error_rate capacity [EXPANSION expansion] [NONSCALING]
 * <p>
 * Creates an empty Bloom filter with a single sub-filter for the initial specified capacity
 * and with an upper bound error_rate.
 *
 * @see <a href="https://redis.io/commands/bf.reserve/">BF.RESERVE</a>
 * @since 16.2
 */
public class BFRESERVE extends RespCommand implements Resp3Command {

   public BFRESERVE() {
      super("BF.RESERVE", -4, 1, 1, 1,
            AclCategory.BLOOM.mask() | AclCategory.WRITE.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      double errorRate = toDouble(arguments.get(1));
      long capacity = toLong(arguments.get(2));

      if (errorRate <= 0 || errorRate >= 1) {
         handler.writer().customError("ERR (0 < error rate range < 1)");
         return handler.myStage();
      }

      if (capacity <= 0) {
         handler.writer().customError("ERR (capacity should be larger than 0)");
         return handler.myStage();
      }

      int expansion = BloomFilter.DEFAULT_EXPANSION;
      boolean nonScaling = false;

      for (int i = 3; i < arguments.size(); i++) {
         String arg = new String(arguments.get(i), StandardCharsets.US_ASCII).toUpperCase();
         switch (arg) {
            case "EXPANSION":
               if (i + 1 >= arguments.size()) {
                  handler.writer().syntaxError();
                  return handler.myStage();
               }
               expansion = toInt(arguments.get(++i));
               if (expansion <= 0) {
                  handler.writer().customError("ERR (expansion should be larger than 0)");
                  return handler.myStage();
               }
               break;
            case "NONSCALING":
               nonScaling = true;
               break;
            default:
               handler.writer().syntaxError();
               return handler.myStage();
         }
      }

      FunctionalMap.ReadWriteMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadWriteMap();

      BloomFilterReserveFunction function = new BloomFilterReserveFunction(errorRate, capacity, expansion, nonScaling);
      CompletionStage<Boolean> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, ResponseWriter.OK);
   }
}
