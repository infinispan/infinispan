package org.infinispan.server.resp.commands.tx;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.TransactionResp3Command;
import org.infinispan.server.resp.tx.RespTransactionHandler;

import io.netty.channel.ChannelHandlerContext;

/**
 * `<code>DISCARD</code>` command.
 * <p>
 * This command clears the queued commands and returns to the normal mode. All the registered listeners are
 * removed.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/discard">Redis Documentation</a>
 * @author José Bolina
 */
public class DISCARD extends RespCommand implements Resp3Command, TransactionResp3Command {

   public DISCARD() {
      super(1, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      RespErrorUtil.customError("DISCARD without MULTI", handler.allocator());
      return handler.myStage();
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(RespTransactionHandler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      Resp3Handler next = handler.respServer().newHandler();
      return handler.stageToReturn(handler.dropTransaction(ctx), ctx, ignore -> {
         Consumers.OK_BICONSUMER.accept(null, handler.allocator());
         return next;
      });
   }
}
