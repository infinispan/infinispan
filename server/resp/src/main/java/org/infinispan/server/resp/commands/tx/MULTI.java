package org.infinispan.server.resp.commands.tx;

import java.util.List;
import java.util.concurrent.CompletableFuture;
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
 * `<code>MULTI</code>` command.
 * <p>
 * This command marks the start of a transaction block. Subsequent commands will be queued for execution, and always
 * replying with a {@link org.infinispan.server.resp.RespConstants#QUEUED_REPLY} response. The commands are verified
 * for errors, such as number of arguments, and an error is returned instead. Although, the transaction is not aborted.
 * <p>
 * Sending "nested" MULTI commands is not accepted, that is, sending a MULTI command while inside a transaction. Again,
 * this does not abort the transaction, but returns an error.
 * <p>
 * Redis also does not include a rollback command. Instead, the user can send a DISCARD command to abort the transaction.
 * This means the queued commands are discarded and the transaction context ends. Another commands with a similar
 * behavior are the {@link org.infinispan.server.resp.commands.connection.QUIT} and the subscription commands
 * {@link org.infinispan.server.resp.commands.pubsub.SUBSCRIBE} and {@link org.infinispan.server.resp.commands.pubsub.PSUBSCRIBE}.
 * The subscription commands drop the queued commands and enter into pub-sub mode.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/multi/">Redis documentation</a>
 * @see <a href="https://redis.io/docs/interact/transactions/">Redis transactions documentation</a>
 * @author José Bolina
 */
public class MULTI extends RespCommand implements Resp3Command, TransactionResp3Command {

   public MULTI() {
      super(1, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      Consumers.OK_BICONSUMER.accept(null, handler.allocator());
      return CompletableFuture.completedFuture(new RespTransactionHandler(handler.respServer()));
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(RespTransactionHandler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      RespErrorUtil.customError("MULTI calls can not be nested", handler.allocator());
      return handler.myStage();
   }
}
