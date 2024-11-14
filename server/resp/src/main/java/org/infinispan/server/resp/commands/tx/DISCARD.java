package org.infinispan.server.resp.commands.tx;

import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.AclCategory;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.TransactionResp3Command;
import org.infinispan.server.resp.tx.RespTransactionHandler;

import io.netty.channel.ChannelHandlerContext;

/**
 * DISCARD
 *
 * @author José Bolina
 * @see <a href="https://redis.io/commands/discard/">DISCARD</a>
 * @since 15.0
 */
public class DISCARD extends RespCommand implements Resp3Command, TransactionResp3Command {

   public DISCARD() {
      super(1, 0, 0, 0);
   }

   @Override
   public long aclMask() {
      return AclCategory.FAST | AclCategory.TRANSACTION;
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      handler.writer().customError("DISCARD without MULTI");
      return handler.myStage();
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(RespTransactionHandler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      Resp3Handler next = handler.respServer().newHandler(handler.cache());
      return handler.stageToReturn(handler.dropTransaction(ctx), ctx, ignore -> {
         handler.writer().ok();
         return next;
      });
   }
}
