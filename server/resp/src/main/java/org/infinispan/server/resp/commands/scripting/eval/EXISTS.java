package org.infinispan.server.resp.commands.scripting.eval;

import static org.infinispan.server.resp.RespUtil.ascii;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.serialization.Resp3Type;

import io.netty.channel.ChannelHandlerContext;

/**
 * SCRIPT EXISTS
 *
 * @see <a href="https://redis.io/docs/latest/commands/script-exists/">SCRIPT EXISTS</a>
 * @since 15.1
 */
public class EXISTS extends RespCommand implements Resp3Command {
   protected EXISTS() {
      super(-3, 0, 0, 0, AclCategory.SCRIPTING.mask() | AclCategory.SLOW.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      List<String> shas = new ArrayList<>();
      for (int i = 1; i < arguments.size(); i++) {
         shas.add(ascii(arguments.get(i)));
      }
      try {
         return handler.getBlockingManager()
               .supplyBlocking(() -> handler.respServer().luaEngine().scriptExists(shas), "script exists")
               .thenApplyAsync(exists -> {
                  handler.writer().array(exists, Resp3Type.INTEGER);
                  return handler;
               }, ctx.channel().eventLoop());
      } catch (Exception e) {
         handler.writer().customError(e.getMessage());
         return handler.myStage();
      }
   }
}
