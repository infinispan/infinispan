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
      super(-2, 0, 0, 0, AclCategory.FAST.mask() | AclCategory.CONNECTION.mask());
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3AuthHandler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      CompletionStage<Void> successStage = handler.performAuth(ctx, arguments.get(0), arguments.get(1));

      return handler.stageToReturn(successStage, ctx, ignore -> createAfterAuthentication(handler));
   }

   static RespRequestHandler createAfterAuthentication(Resp3AuthHandler prev) {
      RespRequestHandler next = silentCreateAfterAuthentication(prev);
      if (next == null)
         return prev;

      prev.writer().ok();
      return next;
   }

   static RespRequestHandler silentCreateAfterAuthentication(Resp3AuthHandler prev) {
      return prev.respServer().newHandler(prev.cache());
   }
}
