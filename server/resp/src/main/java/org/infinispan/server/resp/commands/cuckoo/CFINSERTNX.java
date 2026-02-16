package org.infinispan.server.resp.commands.cuckoo;

import static org.infinispan.server.resp.commands.ArgumentUtils.toLong;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.functional.FunctionalMap;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Type;

import io.netty.channel.ChannelHandlerContext;

/**
 * CF.INSERTNX key [CAPACITY capacity] [NOCREATE] ITEMS item [item ...]
 * <p>
 * Adds one or more items to a Cuckoo filter only if they don't already exist.
 *
 * @see <a href="https://redis.io/commands/cf.insertnx/">CF.INSERTNX</a>
 * @since 16.2
 */
public class CFINSERTNX extends RespCommand implements Resp3Command {

   public CFINSERTNX() {
      super("CF.INSERTNX", -4, 1, 1, 1,
            AclCategory.CUCKOO.mask() | AclCategory.WRITE.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      long capacity = CuckooFilter.DEFAULT_CAPACITY;
      boolean noCreate = false;
      List<byte[]> items = null;

      int i = 1;
      while (i < arguments.size()) {
         String arg = new String(arguments.get(i), StandardCharsets.US_ASCII).toUpperCase();
         switch (arg) {
            case "CAPACITY":
               if (i + 1 >= arguments.size()) {
                  handler.writer().syntaxError();
                  return handler.myStage();
               }
               capacity = toLong(arguments.get(++i));
               if (capacity <= 0) {
                  handler.writer().customError("ERR (capacity should be larger than 0)");
                  return handler.myStage();
               }
               break;
            case "NOCREATE":
               noCreate = true;
               break;
            case "ITEMS":
               items = new ArrayList<>(arguments.subList(i + 1, arguments.size()));
               i = arguments.size();
               break;
            default:
               handler.writer().syntaxError();
               return handler.myStage();
         }
         i++;
      }

      if (items == null || items.isEmpty()) {
         handler.writer().customError("ERR ITEMS keyword must be provided with at least one item");
         return handler.myStage();
      }

      FunctionalMap.ReadWriteMap<byte[], Object> cache =
            FunctionalMap.create(handler.typedCache(null)).toReadWriteMap();

      CuckooFilterInsertFunction function = new CuckooFilterInsertFunction(items, capacity, noCreate, true);
      CompletionStage<List<Integer>> result = cache.eval(key, function);

      return handler.stageToReturn(result, ctx, (r, w) ->
            w.array(r, Resp3Type.INTEGER));
   }
}
