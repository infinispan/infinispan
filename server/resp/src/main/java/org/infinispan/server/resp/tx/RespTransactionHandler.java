package org.infinispan.server.resp.tx;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.logging.LogFactory;
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
import org.infinispan.server.resp.logging.Log;

import io.netty.channel.ChannelHandlerContext;

/**
 * Handles the commands while in the transaction state.
 * <p>
 * The transaction state is entered when the {@link org.infinispan.server.resp.commands.tx.MULTI} command is received
 * and exited when the EXEC or DISCARD command is received. Additional commands that change the handler include the
 * quit and subscribe commands.
 * <p>
 * The handler will queue all commands and copy the arguments received.
 *
 * @see <a href="https://redis.io/docs/interact/transactions/">Redis transactions documentation</a>
 * @author José Bolina
 */
public class RespTransactionHandler extends CacheRespRequestHandler {

   private static final Log log = LogFactory.getLog(RespTransactionHandler.class, Log.class);
   private final List<TransactionCommand> queued;

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
         dropTransaction();
         SubscriberHandler subscriberHandler = new SubscriberHandler(respServer(), respServer().newHandler());
         return subscriberHandler.handleRequest(ctx, command, arguments);
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

      queued.add(new TransactionCommand(command, Collections.unmodifiableList(arguments)));
      return stageToReturn(myStage(), ctx, Consumers.QUEUED_BICONSUMER);
   }

   @Override
   public void handleChannelDisconnect(ChannelHandlerContext ctx) {
      dropTransaction();
   }

   private boolean isCommandValid(RespCommand command, List<byte[]> arguments) {
      if (!command.hasValidNumberOfArguments(arguments)) {
         RespErrorUtil.wrongArgumentNumber(command, allocator());
         return false;
      }

      return true;
   }

   public void dropTransaction() {
      queued.clear();
   }
}
