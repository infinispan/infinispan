package org.infinispan.server.resp.tx;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.server.resp.CacheRespRequestHandler;
import org.infinispan.server.resp.Consumers;
import org.infinispan.server.resp.RespCommand;
import org.infinispan.server.resp.RespErrorUtil;
import org.infinispan.server.resp.RespRequestHandler;
import org.infinispan.server.resp.RespServer;
import org.infinispan.server.resp.SubscriberHandler;
import org.infinispan.server.resp.commands.TransactionResp3Command;
import org.infinispan.server.resp.commands.pubsub.PSUBSCRIBE;
import org.infinispan.server.resp.commands.pubsub.SUBSCRIBE;
import org.infinispan.server.resp.commands.tx.UNWATCH;
import org.infinispan.server.resp.commands.tx.WATCH;

import io.netty.channel.ChannelHandlerContext;

/**
 * Handles the commands while in the transaction state.
 * <p>
 * The transaction state is entered when the {@link org.infinispan.server.resp.commands.tx.MULTI} command is received
 * and exited when the {@link org.infinispan.server.resp.commands.tx.EXEC} or DISCARD command is received. Additional
 * commands that change the handler include {@link org.infinispan.server.resp.commands.connection.QUIT} and the
 * subscribe commands.
 * <p>
 * The handler will queue all commands and copy the arguments received. Observe that, this information is kept in-memory,
 * so it will not survive restarts. If the node stops in some way and come back, the queued commands are lost. In the
 * same way, it does not offer atomicity during execution. If the server stops mid-way applying operations, it does not
 * recover, leaving a half-applied transaction.
 * <p>
 * Talking in isolation levels, this implementation is next to read-uncommitted. Another client could see the state of
 * the transaction before it is finished, even on a single node.
 *
 * @see <a href="https://redis.io/docs/interact/transactions/">Redis transactions documentation</a>
 * @author José Bolina
 */
public class RespTransactionHandler extends CacheRespRequestHandler {

   private final List<TransactionCommand> queued;
   private boolean failed;

   public RespTransactionHandler(RespServer respServer) {
      super(respServer);
      this.queued = new ArrayList<>();
   }

   @Override
   protected CompletionStage<RespRequestHandler> actualHandleRequest(ChannelHandlerContext ctx, RespCommand command, List<byte[]> arguments) {
      initializeIfNecessary(ctx);
      // Subscribe commands discard the queue and enter into pub-sub. See: https://github.com/redis/redis/pull/9928
      // Doing specific checks here instead of implementing on the commands, so we can update this later, if necessary.
      if (command instanceof SUBSCRIBE || command instanceof PSUBSCRIBE) {
         CompletionStage<?> drop = dropTransaction(ctx);
         SubscriberHandler subscriberHandler = new SubscriberHandler(respServer(), respServer().newHandler());
         return subscriberHandler.handleRequest(ctx, command, arguments).thenCombine(drop, (handler, ignore) -> handler);
      }

      // Transaction commands take precedence and are not queued.
      // We need to verify how commands like WATCH are handled from within a transaction.
      if (command instanceof TransactionResp3Command) {
         TransactionResp3Command tx = (TransactionResp3Command) command;
         return tx.perform(this, ctx, arguments);
      }

      // Queued commands need to be parsed for syntax errors. Redis verify the number of arguments.
      // This method already writes any error message to the socket.
      if (!isCommandValid(command, arguments)) return myStage();

      try {
         queued.add(new TransactionCommand(command, List.copyOf(arguments)));
      } catch (Throwable t) {
         errorInTransactionContext();
         return command.handleException(this, t);
      }

      return stageToReturn(myStage(), ctx, Consumers.QUEUED_BICONSUMER);
   }

   @Override
   public void handleChannelDisconnect(ChannelHandlerContext ctx) {
      dropTransaction(ctx);
   }

   private boolean isCommandValid(RespCommand command, List<byte[]> arguments) {
      if (!command.hasValidNumberOfArguments(arguments)) {
         RespErrorUtil.wrongArgumentNumber(command, allocator());
         return false;
      }

      return true;
   }

   public void errorInTransactionContext() {
      this.failed = true;
   }

   public boolean hasFailed() {
      return failed;
   }

   public CompletionStage<?> dropTransaction(ChannelHandlerContext ctx) {
      queued.clear();
      return unregisterListeners(ctx);
   }

   /**
    * Prepare for performing the queued operations.
    * <p>
    * This method verifies for possible watched keys to verify if the operation needs to be aborted. A <code>null</code>
    * return means the transaction needs to be aborted. After this execution, all keys are un-watched, removing the
    * listeners from the cache.
    *
    * @return The list of commands to be executed, or <code>null</code> if the transaction needs to be aborted.
    */
   public CompletionStage<List<TransactionCommand>> performingOperations(ChannelHandlerContext ctx) {
      return unregisterListeners(ctx).thenApply(watchers -> {
         if (watchers != null) {
            for (WATCH.TxKeysListener watcher : watchers) {
               if (watcher.hasSeenEvents()) return null;
            }
         }

         return queued;
      });
   }

   public CompletionStage<List<WATCH.TxKeysListener>> unregisterListeners(ChannelHandlerContext ctx) {
      return UNWATCH.deregister(ctx, cache());
   }
}
