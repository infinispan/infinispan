package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.statetransfer.ConflictResolutionStartCommand;
import org.infinispan.commands.statetransfer.StateTransferCancelCommand;
import org.infinispan.commands.statetransfer.StateTransferGetListenersCommand;
import org.infinispan.commands.statetransfer.StateTransferGetTransactionsCommand;
import org.infinispan.commands.statetransfer.StateTransferStartCommand;
import org.infinispan.util.concurrent.BlockingRunnable;

/**
 * A {@link PerCacheInboundInvocationHandler} implementation for non-total order caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class TxPerCacheInboundInvocationHandler extends BasePerCacheInboundInvocationHandler {

   @Override
   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      try {
         final int commandTopologyId = extractCommandTopologyId(command);
         final boolean onExecutorService = executeOnExecutorService(order, command);
         final boolean sync = order.preserveOrder();
         final BlockingRunnable runnable;
         switch (command.getCommandId()) {
            case ConflictResolutionStartCommand.COMMAND_ID:
            case StateTransferCancelCommand.COMMAND_ID:
            case StateTransferGetListenersCommand.COMMAND_ID:
            case StateTransferGetTransactionsCommand.COMMAND_ID:
            case StateTransferStartCommand.COMMAND_ID:
               runnable = createDefaultRunnable(command, reply, commandTopologyId, false, onExecutorService, sync);
               break;
            default:
               runnable = createDefaultRunnable(command, reply, commandTopologyId, true, onExecutorService, sync);
         }
         handleRunnable(runnable, onExecutorService);
      } catch (Throwable throwable) {
         reply.reply(exceptionHandlingCommand(command, throwable));
      }
   }

}
