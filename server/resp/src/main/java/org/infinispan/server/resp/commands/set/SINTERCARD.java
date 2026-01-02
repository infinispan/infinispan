package org.infinispan.server.resp.commands.set;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.multimap.impl.EmbeddedSetCache;
import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.ArgumentUtils;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * SINTERCARD
 *
 * @see <a href="https://redis.io/commands/sintercard/">SINTERCARD</a>
 * @since 15.0
 */
public class SINTERCARD extends RespCommand implements Resp3Command {
   static String LIMIT_OPT = "LIMIT";

   public SINTERCARD() {
      super(-3, 0, 0, 0, AclCategory.READ.mask() | AclCategory.SET.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
         ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      EmbeddedSetCache<byte[], byte[]> esc = handler.getEmbeddedSetCache();
      int keysNum = 0;
      try {
         keysNum = ArgumentUtils.toInt(arguments.get(0));
      } catch (NumberFormatException ignore) {
      }

      // Wrong numKey value
      if (keysNum==0) {
         handler.writer().customError("numkeys should be greater than 0");
         return handler.myStage();
      }

      final int limit = processArgs(keysNum, arguments, handler);
      if (limit < 0) { // Wrong args
         return handler.myStage();
      }
      var keys = arguments.subList(1, keysNum + 1);
      var uniqueKeys = SINTER.getUniqueKeys(handler, keys);
      var allEntries= esc.getAll(uniqueKeys);
      return handler.stageToReturn(allEntries.thenApply((sets) -> sets.size() == uniqueKeys.size() ? (long) SINTER.intersect(sets.values(), limit).size() : SINTER.checkTypesAndReturnEmpty(sets.values()).size()),
            ctx,
            ResponseWriter.INTEGER);
   }

   private int processArgs(int keysNum, List<byte[]> arguments, Resp3Handler handler) {
      // Wrong args num
      if (arguments.size() < keysNum + 1) {
         handler.writer().customError("Number of keys can't be greater than number of args");
         return -1;
      }
      int optVal = 0;
      if (arguments.size() > keysNum + 1) {
         if (arguments.size() != keysNum + 3) {
            // Options provided but wrong arg nums
            handler.writer().syntaxError();
            return -1;
         }
         var opt = new String(arguments.get(keysNum + 1)).toUpperCase();
         if (!LIMIT_OPT.equals(opt)) {
            // Wrong option provided
            handler.writer().syntaxError();
            return -1;
         }
         try {
            optVal = ArgumentUtils.toInt(arguments.get(keysNum + 2));
            if (optVal < 0) {
               // Negative limit provided
               handler.writer().customError("LIMIT can't be negative");
               return -1;
            }
         } catch (NumberFormatException ex) {
            // Limit provided not an integer. sending same message as Redis
            handler.writer().customError("LIMIT can't be negative");
            return -1;
         }
      }
      return optVal;
   }
}
