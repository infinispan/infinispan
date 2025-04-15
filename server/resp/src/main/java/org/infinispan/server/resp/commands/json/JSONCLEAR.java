package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.json.JSONUtil;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * JSON.CLEAR
 *
 * @see <a href="https://redis.io/commands/json.clear/">JSON.CLEAR</a>
 * @since 15.2
 */
public class JSONCLEAR extends RespCommand implements Resp3Command {

   private final byte[] DEFAULT_PATH = { '.' };

   public JSONCLEAR() {
      super("JSON.CLEAR", -1, 1, 1, 1, AclCategory.JSON.mask() | AclCategory.WRITE.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      byte[] key = arguments.get(0);
      byte[] path = arguments.size() > 1 ? arguments.get(1): DEFAULT_PATH;
      byte[] jsonPath  = JSONUtil.toJsonPath(path);
      EmbeddedJsonCache ejc = handler.getJsonCache();
      CompletionStage<Integer> result = ejc.clear(key, jsonPath);
      return handler.stageToReturn(result, ctx, ResponseWriter.INTEGER);
   }
}
