package org.infinispan.server.resp.commands.scripting.eval;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * SCRIPT FLUSH
 *
 * @see <a href="https://redis.io/docs/latest/commands/script-flush/">SCRIPT FLUSH</a>
 * @since 15.2
 */
public class FLUSH extends RespCommand implements Resp3Command {
   protected FLUSH() {
      super(-2, 0, 0, 0);
   }

   @Override
   public long aclMask() {
      return AclCategory.SCRIPTING | AclCategory.SLOW;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      try {
         return handler.getBlockingManager()
               .supplyBlocking(() -> {
                  handler.respServer().luaEngine().scriptFlush();
                  return null;
               }, "script flush")
               .thenApplyAsync(__ -> {
                  handler.writer().ok();
                  return handler;
               }, ctx.channel().eventLoop());
      } catch (Exception e) {
         handler.writer().customError(e.getMessage());
         return handler.myStage();
      }
   }
}
