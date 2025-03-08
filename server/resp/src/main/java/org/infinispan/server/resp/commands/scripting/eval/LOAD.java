package org.infinispan.server.resp.commands.scripting.eval;

import static org.infinispan.server.resp.RespUtil.ascii;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.scripting.LuaTaskEngine;

import io.netty.channel.ChannelHandlerContext;

/**
 * SCRIPT LOAD
 *
 * @see <a href="https://redis.io/docs/latest/commands/script-load/">SCRIPT LOAD</a>
 * @since 15.1
 */
public class LOAD extends RespCommand implements Resp3Command {
   protected LOAD() {
      super(3, 0, 0, 0);
   }

   @Override
   public long aclMask() {
      return AclCategory.SCRIPTING | AclCategory.SLOW;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      String script = ascii(arguments.get(1));
      try {
         LuaTaskEngine engine = handler.respServer().luaEngine();
         return handler.getBlockingManager()
               .supplyBlocking(() -> engine.scriptLoad(script, true).sha(), "script load")
               .thenApplyAsync(sha -> {
                  handler.writer().string(sha);
                  return handler;
               }, ctx.channel().eventLoop());
      } catch (Exception e) {
         handler.writer().customError(e.getMessage());
         return handler.myStage();
      }
   }
}
