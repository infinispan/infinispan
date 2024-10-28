package org.infinispan.server.resp.commands.tx;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.commons.util.concurrent.CompletionStages;
import org.infinispan.server.resp.Resp3Handler;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.commands.Resp3Command;
import org.infinispan.server.resp.commands.TransactionResp3Command;
import org.infinispan.server.resp.serialization.ByteBufferUtils;
import org.infinispan.server.resp.serialization.Resp3Response;
import org.infinispan.server.resp.serialization.RespConstants;
import org.infinispan.server.resp.tx.RespTransactionHandler;
import org.infinispan.server.resp.tx.TransactionCommand;
import org.infinispan.server.resp.tx.TransactionContext;

import io.netty.channel.ChannelHandlerContext;

/**
 * `<code>EXEC</code>` command.
 * <p>
 * Retrieves the queued commands from the handler and executes them serially. If the handler returns a <code>null</code>
 * list, the transaction is aborted. The user installed a watch for a key, and the value was updated.
 * <p>
 * This implementation executes the operations outside a transaction context on the Infinispan level. Another user can
 * see the state changing before the queue finishes. Even though Redis does not have the concept of commit/rollback,
 * which means that everything is applied, the current implementation provides a weaker isolation level.
 *
 * @since 15.0
 * @see <a href="https://redis.io/commands/exec">Redis Documentation</a>
 * @author José Bolina
 */
public class EXEC extends RespCommand implements Resp3Command, TransactionResp3Command {

   public EXEC() {
      super(1, 0, 0, 0);
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(Resp3Handler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      RespErrorUtil.customError("EXEC without MULTI", handler.allocator());
      return handler.myStage();
   }

   @Override
   public CompletionStage<RespRequestHandler> perform(RespTransactionHandler handler, ChannelHandlerContext ctx, List<byte[]> arguments) {
      Resp3Handler next = handler.respServer().newHandler(handler.cache());
      CompletionStage<?> cs = handler.performingOperations(ctx)
            .thenCompose(commands -> perform(commands, handler, next, ctx));
      return next.stageToReturn(cs, ctx, ignore -> next);
   }

   private CompletionStage<Void> perform(List<TransactionCommand> commands, RespTransactionHandler curr, Resp3Handler next, ChannelHandlerContext ctx) {
      // An error happened while in transaction context. This error means more of a setup error, failed enqueueing, etc.
      // One such example, trying to WATCH while in transaction context. This is not a command failed to execute yet.
      // See: https://redis.io/docs/interact/transactions/#errors-inside-a-transaction
      if (curr.hasFailed()) {
         return CompletableFuture.supplyAsync(() -> {
            RespErrorUtil.transactionAborted(curr.allocator());
            return null;
         }, ctx.executor());
      }

      // Using WATCH and keys changed. Abort transaction.
      if (commands == null) {
         // Should write `(nil)` since the transaction is aborted.
         return CompletableFuture.supplyAsync(() -> {
            Resp3Response.nulls(curr.allocator());
            return null;
         }, ctx.executor());
      }

      AdvancedCache<byte[], byte[]> cache = next.cache();

      // Redis has a serializable isolation. Verify the cache is transactional to manually create the context.
      boolean transactional = cache.getCacheConfiguration().transaction().transactionMode().isTransactional();
      final TransactionDecorator.TransactionResume resume;
      if (!transactional) {
         log.enableTransactionForMultiExec();
         resume = null;
      } else {
         resume = TransactionDecorator.beginTransaction(next, cache);
      }
      return CompletableFuture.supplyAsync(() -> {
         // Mark the commands are executing from within a transaction context.
         TransactionContext.startTransactionContext(ctx);

         // Unfortunately, we need to manually write the prefix before proceeding with each operation.
         ByteBufferUtils.writeNumericPrefix(RespConstants.ARRAY, commands.size(), curr.allocator());
         return CompletionStages.handleAndCompose(orderlyExecution(next, ctx, commands), (ignore, t) -> {
            TransactionContext.endTransactionContext(ctx);
            return transactional
                  ? TransactionDecorator.completeTransaction(resume, t == null)
                  : CompletableFutures.completedNull();
         });
      }, ctx.executor()).thenCompose(Function.identity());
   }

   private CompletionStage<Void> orderlyExecution(Resp3Handler handler, ChannelHandlerContext ctx,
                                               List<TransactionCommand> commands) {
      return CompletionStages.performSequentially(commands.iterator(),
            cmd -> cmd.perform(handler, ctx).thenApply(CompletableFutures.toNullFunction()));
   }
}
