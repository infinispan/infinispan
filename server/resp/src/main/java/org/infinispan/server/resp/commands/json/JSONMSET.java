package org.infinispan.server.resp.commands.json;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespUtil;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * JSON.MSET
 *
 * @see <a href="https://redis.io/commands/json.mset/">JSON.MSET</a>
 *
 *      Note: this version is non atomic
 * @since 15.2
 */
public class JSONMSET extends RespCommand implements Resp3Command {

   public JSONMSET() {
      super("JSON.MSET", -4, 1, -1, 3);
   }

   @Override
   public long aclMask() {
      return 0;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      if (arguments.size() % 3 != 0) {
         handler.writer().syntaxError();
         return handler.myStage();
      }
      List<byte[][]> argsList = new ArrayList<>();
      for (int i = 0; i < arguments.size(); i += 3) {
         if (isValueInvalid(arguments.get(i + 2))) {
            handler.writer().customError("Invalid json value for JSON.MSET");
            return handler.myStage();
         }
         argsList.add(new byte[][] { arguments.get(i), arguments.get(i + 1), arguments.get(i + 2) });
      }
      EmbeddedJsonCache ejc = handler.getJsonCache();
      CompletionStage<Void> results = CompletionStages.performSequentially(argsList.iterator(),
            args -> ejc.set(args[0], args[2], args[1], false, false).handle((res, ex) -> handleWrongTypeError(ex)));
      return handler.stageToReturn(results, ctx, ResponseWriter.OK);
   }

   private static Void handleWrongTypeError(Throwable ex) {
      if (ex==null || RespUtil.isWrongTypeError(ex)) {
         return null;
      }
      throw CompletableFutures.asCompletionException(ex);
   }

   // Invalid values for Redis. Expecially '\0xa' breaks RESP, seen as end of data
   private boolean isValueInvalid(byte[] value) {
      if (value.length == 0)
         return true;
      if (value.length == 1) {
         switch (value[0]) {
            case ' ':
            case '{':
            case '}':
            case '[':
            case ']':
            case '\\':
            case '\'':
            case 0:
            case 0x0a:
            case 0x0c:
               return true;
            default:
               return false;
         }
      }
      if (value.length == 2) {
         if (value[0] == '\\' && (value[1] == '\\' || value[1] == '"' || value[1] == '[')) {
            return true;
         }
         if (value[0] == '{' && value[1] == ']') {
            return true;
         }
         if (value[0] == '[' && value[1] == '}') {
            return true;
         }
      }
      return false;
   }
}
