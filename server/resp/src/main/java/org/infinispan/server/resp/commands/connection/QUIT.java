package org.infinispan.server.resp.commands.connection;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Resp3AuthHandler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.SubscriberHandler;
import org.infinispan.server.resp.commands.AuthResp3Command;
import org.infinispan.server.resp.commands.PubSubResp3Command;
import org.infinispan.server.resp.commands.TransactionResp3Command;
import org.infinispan.server.resp.tx.RespTransactionHandler;

import io.netty.channel.ChannelHandlerContext;

/**
 * @link https://redis.io/commands/quit/
 * @since 14.0
 */
public class QUIT extends RespCommand implements AuthResp3Command, PubSubResp3Command, TransactionResp3Command {

   public QUIT() {
      super(1, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3AuthHandler handler,
                                                      ChannelHandlerContext ctx,
                                                      List<byte[]> arguments) {
      ctx.close();
      return handler.myStage();
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(SubscriberHandler handler, ChannelHandlerContext ctx,
                                                                List<byte[]> arguments) {
      handler.removeAllListeners();
      return handler.resp3Handler().handleRequest(ctx, this, arguments);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(RespTransactionHandler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      return handler.dropTransaction(ctx)
            .thenCompose(ignore -> handler.respServer().newHandler().handleRequest(ctx, this, arguments));
   }
}
