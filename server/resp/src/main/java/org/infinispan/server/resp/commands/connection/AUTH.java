package org.infinispan.server.resp.commands.connection;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3AuthHandler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.AuthResp3Command;
import org.infinispan.server.resp.serialization.Resp3Response;

import io.netty.channel.ChannelHandlerContext;

/**
 * @see <a href="https://redis.io/commands/auth/">Redis documentation</a>
 * @since 14.0
 */
public class AUTH extends RespCommand implements AuthResp3Command {
   public AUTH() {
      super(-2, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3AuthHandler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      CompletionStage<Boolean> successStage = handler.performAuth(ctx, arguments.get(0), arguments.get(1));

      return handler.stageToReturn(successStage, ctx, success -> createAfterAuthentication(success, handler));
   }

   static RespRequestHandler createAfterAuthentication(boolean success, Resp3AuthHandler prev) {
      if (!success) RespErrorUtil.unauthorized(prev.allocator());
      else Resp3Response.ok(prev.allocator());

      return silentCreateAfterAuthentication(success, prev);
   }

   static RespRequestHandler silentCreateAfterAuthentication(boolean success, Resp3AuthHandler prev) {
      return success ? prev.respServer().newHandler() : prev;
   }
}
