package org.infinispan.server.resp.commands.connection;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.ByteBufferUtils;
import org.infinispan.server.resp.Resp3AuthHandler;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.SubscriberHandler;
import org.infinispan.server.resp.commands.PubSubResp3Command;
import org.infinispan.server.resp.commands.Resp3Command;

import io.netty.channel.ChannelHandlerContext;

/**
 * @link https://redis.io/commands/reset/
 * @since 14.0
 */
public class RESET extends RespCommand implements Resp3Command, PubSubResp3Command {
   public RESET() {
      super(1, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      ByteBufferUtils.stringToByteBufAscii("+RESET\r\n", handler.allocator());
      if (handler.respServer().getConfiguration().authentication().enabled()) {
         return CompletableFuture.completedFuture(new Resp3AuthHandler(handler.respServer()));
      }
      return handler.myStage();
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(SubscriberHandler handler, ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      handler.removeAllListeners();
      return handler.resp3Handler().handleRequest(ctx, this, arguments);
   }
}
