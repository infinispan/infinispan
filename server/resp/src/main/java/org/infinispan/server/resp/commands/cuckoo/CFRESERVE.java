package org.infinispan.server.resp.commands.cuckoo;

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
 * CF.RESERVE key capacity [BUCKETSIZE bucketsize] [MAXITERATIONS maxiterations] [EXPANSION expansion]
 * <p>
 * Creates an empty Cuckoo filter with the specified capacity.
 *
 * @see <a href="https://redis.io/commands/cf.reserve/">CF.RESERVE</a>
 * @since 16.2
 */
public class CFRESERVE extends RespCommand implements Resp3Command {

   public CFRESERVE() {
      super("CF.RESERVE", -3, 1, 1, 1,
            AclCategory.CUCKOO.mask() | AclCategory.WRITE.mask() | AclCategory.FAST.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      if (arguments.size() < 2) {
         handler.writer().wrongArgumentNumber(this);
         return handler.myStage();
      }

      byte[] key = arguments.get(0);
      long capacity = toLong(arguments.get(1));

      if (capacity <= 0) {
         handler.writer().customError("ERR (capacity should be larger than 0)");
         return handler.myStage();
      }

      int bucketSize = CuckooFilter.DEFAULT_BUCKET_SIZE;
      int maxIterations = CuckooFilter.DEFAULT_MAX_ITERATIONS;
      int expansion = CuckooFilter.DEFAULT_EXPANSION;

      for (int i = 2; i < arguments.size(); i++) {
         String arg = new String(arguments.get(i), StandardCharsets.US_ASCII).toUpperCase();
         switch (arg) {
            case "BUCKETSIZE":
               if (i + 1 >= arguments.size()) {
                  handler.writer().syntaxError();
                  return handler.myStage();
               }
               bucketSize = toInt(arguments.get(++i));
               if (bucketSize <= 0 || bucketSize > 255) {
                  handler.writer().customError("ERR (bucket size should be between 1 and 255)");
                  return handler.myStage();
               }
               break;
            case "MAXITERATIONS":
               if (i + 1 >= arguments.size()) {
                  handler.writer().syntaxError();
                  return handler.myStage();
               }
               maxIterations = toInt(arguments.get(++i));
               if (maxIterations <= 0) {
                  handler.writer().customError("ERR (max iterations should be larger than 0)");
                  return handler.myStage();
               }
               break;
            case "EXPANSION":
               if (i + 1 >= arguments.size()) {
                  handler.writer().syntaxError();
                  return handler.myStage();
               }
               expansion = toInt(arguments.get(++i));
               if (expansion < 0) {
                  handler.writer().customError("ERR (expansion should be >= 0)");
                  return handler.myStage();
               }
               break;
            default:
               handler.writer().syntaxError();
               return handler.myStage();
         }
      }

      FunctionalMap.ReadWriteMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadWriteMap();

      CuckooFilterReserveFunction function = new CuckooFilterReserveFunction(capacity, bucketSize, maxIterations, expansion);
      CompletionStage<Boolean> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, ResponseWriter.OK);
   }
}
