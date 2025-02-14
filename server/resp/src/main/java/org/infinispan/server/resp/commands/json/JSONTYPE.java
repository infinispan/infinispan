package org.infinispan.server.resp.commands.json;

import io.netty.channel.ChannelHandlerContext;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.serialization.ResponseWriter;

import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * JSON.TYPE
 *
 * @see <a href="https://redis.io/commands/json.type/">JSON.TYPE</a>
 * @since 15.2
 */
public class JSONTYPE extends RespCommand implements Resp3Command {

   private byte[] DEFAULT_PATH = { '.' };

   public JSONTYPE() {
      super("JSON.TYPE", -1, 1, 1, 1);
   }

   @Override
   public long aclMask() {
      return 0;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      JSONCommandArgumentReader.CommandArgs commandArgs = JSONCommandArgumentReader.readCommandArgs(arguments);
      EmbeddedJsonCache ejc = handler.getJsonCache();
      CompletionStage<List<String>> result = ejc.type(commandArgs.key(), commandArgs.jsonPath());
      if (commandArgs.isLegacy()) {
         return handler.stageToReturn(result.thenApply(r -> {
            // return the first one only
            if (r != null && r.size() > 0) {
               return r.get(0);
            }
            // return null in the other cases
            return null;
         }), ctx, ResponseWriter.SIMPLE_STRING);
      }
      return handler.stageToReturn(result, ctx, ResponseWriter.ARRAY_STRING);
   }
}
