package org.infinispan.server.resp.commands.connection;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3AuthHandler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.AuthResp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * AUTH
 *
 * @see <a href="https://redis.io/commands/auth/">AUTH</a>
 * @since 14.0
 */
public class AUTH extends RespCommand implements AuthResp3Command {
   public AUTH() {
      super(-2, 0, 0, 0);
   }

   @Override
   public long aclMask() {
      return AclCategory.FAST | AclCategory.CONNECTION;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3AuthHandler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      CompletionStage<Boolean> successStage = handler.performAuth(ctx, arguments.get(0), arguments.get(1));

      return handler.stageToReturn(successStage, ctx, success -> createAfterAuthentication(success, handler));
   }

   static RespRequestHandler createAfterAuthentication(boolean success, Resp3AuthHandler prev) {
      RespRequestHandler next = silentCreateAfterAuthentication(success, prev);
      if (next == null)
         return prev;

      if (!success) prev.writer().unauthorized();
      else prev.writer().ok();
      return next;
   }

   static RespRequestHandler silentCreateAfterAuthentication(boolean success, Resp3AuthHandler prev) {
      if (!success) return prev;

      try {
         return prev.respServer().newHandler(prev.cache());
      } catch (SecurityException ignore) {
         prev.writer().unauthorized();
         return null;
      }
   }
}
