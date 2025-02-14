package org.infinispan.server.resp.commands.json;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.json.EmbeddedJsonCache;
import org.infinispan.server.resp.serialization.ResponseWriter;

import io.netty.channel.ChannelHandlerContext;

/**
 * JSON.DEL
 *
 * @see <a href="https://redis.io/commands/json.del/">JSON.DEL</a>
 *
 * @since 15.2
 */
public class JSONDEL extends RespCommand implements Resp3Command {

   private static byte[] JSON_ROOT = new byte[] { '$' };

   public JSONDEL() {
      super("JSON.DEL", -2, 1, 1, 1);
   }

   protected JSONDEL(String name, int arity, int firstKeyPos, int lastKeyPos, int steps) {
      super(name, arity, firstKeyPos, lastKeyPos, steps);
   }

   @Override
   public long aclMask() {
      return 0;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx,
         List<byte[]> arguments) {
      if (arguments.size() > 3) {
         handler.writer().syntaxError();
         return handler.myStage();
      }

      byte[] key = arguments.get(0);
      byte[] path = (arguments.size() >= 2) ? arguments.get(1) : JSON_ROOT;
      EmbeddedJsonCache ejc = handler.getJsonCache();

      CompletionStage<Long> cs = ejc.del(key, path);
      return handler.stageToReturn(cs, ctx, ResponseWriter.INTEGER);
   }
}
