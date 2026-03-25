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

import io.netty.channel.ChannelHandlerContext;

/**
 * BF.INSERT key [CAPACITY capacity] [ERROR error] [EXPANSION expansion] [NOCREATE] [NONSCALING] ITEMS item [item ...]
 * <p>
 * Creates a new Bloom filter if the key does not exist using the specified parameters, then adds items.
 *
 * @see <a href="https://redis.io/commands/bf.insert/">BF.INSERT</a>
 * @since 16.2
 */
public class BFINSERT extends RespCommand implements Resp3Command {

   public static final String ERR_WRONG_ARG_NUMBER = "ERR wrong number of arguments for 'bf.insert' command";

   public BFINSERT() {
      super("BF.INSERT", -4, 1, 1, 1,
            AclCategory.BLOOM.mask() | AclCategory.WRITE.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      long capacity = BloomFilter.DEFAULT_CAPACITY;
      double errorRate = BloomFilter.DEFAULT_ERROR_RATE;
      int expansion = BloomFilter.DEFAULT_EXPANSION;
      boolean noCreate = false;
      boolean nonScaling = false;
      List<byte[]> items = null;

      int i = 1;
      while (i < arguments.size()) {
         String arg = new String(arguments.get(i), StandardCharsets.US_ASCII).toUpperCase();
         switch (arg) {
            case "CAPACITY":
               if (i + 1 >= arguments.size()) {
                  handler.writer().customError(ERR_WRONG_ARG_NUMBER);
                  return handler.myStage();
               }
               capacity = toLong(arguments.get(++i));
               if (capacity <= 0) {
                  handler.writer().customError("Bad capacity");
                  return handler.myStage();
               }
               break;
            case "ERROR":
               if (i + 1 >= arguments.size()) {
                  handler.writer().syntaxError();
                  return handler.myStage();
               }
               errorRate = toDouble(arguments.get(++i));
               if (errorRate <= 0 || errorRate >= 1) {
                  handler.writer().customError("Bad error rate");
                  return handler.myStage();
               }
               break;
            case "EXPANSION":
               if (i + 1 >= arguments.size()) {
                  handler.writer().customError(ERR_WRONG_ARG_NUMBER);
                  return handler.myStage();
               }
               expansion = toInt(arguments.get(++i));
               if (expansion <= 0) {
                  handler.writer().customError("Bad expansion");
                  return handler.myStage();
               }
               break;
            case "NOCREATE":
               noCreate = true;
               break;
            case "NONSCALING":
               nonScaling = true;
               break;
            case "ITEMS":
               if (i + 1 >= arguments.size()) {
                  handler.writer().customError(ERR_WRONG_ARG_NUMBER);
                  return handler.myStage();
               }
               items = arguments.subList(i + 1, arguments.size());
               i = arguments.size();
               break;
            default:
               handler.writer().syntaxError();
               return handler.myStage();
         }
         i++;
      }

      if (items == null || items.isEmpty()) {
         handler.writer().customError(ERR_WRONG_ARG_NUMBER);
         return handler.myStage();
      }

      // NOCREATE cannot be combined with CAPACITY or ERROR
      if (noCreate && (capacity != BloomFilter.DEFAULT_CAPACITY || errorRate != BloomFilter.DEFAULT_ERROR_RATE)) {
         handler.writer().customError("ERR NOCREATE cannot be used together with CAPACITY or ERROR");
         return handler.myStage();
      }

      FunctionalMap.ReadWriteMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadWriteMap();

      BloomFilterInsertFunction function = new BloomFilterInsertFunction(items, capacity, errorRate, expansion, noCreate, nonScaling);
      CompletionStage<List<Boolean>> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (r, w) ->
            w.array(r, (b, writer) -> writer.booleans(b)));
   }
}
