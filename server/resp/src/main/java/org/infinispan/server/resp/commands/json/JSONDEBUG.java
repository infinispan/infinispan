package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.connection.MemoryEntrySizeUtils;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * JSON.DEBUG MEMORY
 *
 * @see <a href="https://redis.io/commands/json.debug-memory/">JSON.DEBUG MEMORY</a>
 *
 * @since 15.2
 */
public class JSONDEBUG extends RespCommand implements Resp3Command {

   public JSONDEBUG() {
      super("JSON.DEBUG", -2, 1, 1, 1);
   }

   protected JSONDEBUG(String name, int arity, int firstKeyPos, int lastKeyPos, int steps) {
      super(name, arity, firstKeyPos, lastKeyPos, steps);
   }

   @Override
   public long aclMask() {
      return 0;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      String subcommand = new String(arguments.get(0)).toUpperCase();
      if (subcommand.equals("HELP")) {
         List<String> help = List.of(
                 "MEMORY <key> [path] - reports memory usage",
                 "HELP                - this message");
         return handler.stageToReturn(CompletableFuture.completedFuture(help),
                 ctx, ResponseWriter.ARRAY_STRING);
      }

      if (subcommand.equals("MEMORY")) {
         byte[] key = arguments.get(1);
         JSONCommandArgumentReader.CommandArgs commandArgs = JSONCommandArgumentReader.readCommandArgs(arguments, key, 2);
         EmbeddedJsonCache ejc = handler.getJsonCache();
         if (commandArgs.isRoot()) {
            CompletionStage<CacheEntry<byte[], Object>> cs = handler.typedCache(null).getCacheEntryAsync(key);
            CompletionStage<Long> cs1 = cs
                    .thenApply(e -> e == null ? 0L : MemoryEntrySizeUtils.calculateSize(key, (InternalCacheEntry<byte[], Object>) e));

            if (commandArgs.isLegacy()) {
               return  handler.stageToReturn(cs1, ctx, ResponseWriter.INTEGER);
            }

            return  handler.stageToReturn(cs1.thenApply(size -> List.of(size)), ctx, ResponseWriter.ARRAY_INTEGER);
         }

         CompletionStage<List<Integer>> debug = ejc.debug(key, commandArgs.jsonPath());
         if (commandArgs.isLegacy()) {
            CompletionStage<Integer> cs = debug.thenApply(result -> result.isEmpty() ? 0 : result.get(0));
            return  handler.stageToReturn(cs, ctx, ResponseWriter.INTEGER);
         }
         return  handler.stageToReturn(debug, ctx, ResponseWriter.ARRAY_INTEGER);
      }

      throw new RuntimeException("unknown subcommand - try `JSON.DEBUG HELP`");
   }
}
