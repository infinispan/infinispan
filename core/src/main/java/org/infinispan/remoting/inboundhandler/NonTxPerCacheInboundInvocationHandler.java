package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.statetransfer.ConflictResolutionStartCommand;
import org.infinispan.commands.statetransfer.StateTransferCancelCommand;
import org.infinispan.commands.statetransfer.StateTransferGetListenersCommand;
import org.infinispan.commands.statetransfer.StateTransferGetTransactionsCommand;
import org.infinispan.commands.statetransfer.StateTransferStartCommand;
import org.infinispan.util.concurrent.BlockingRunnable;

/**
 * A {@link org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler} implementation for non-total order
 * caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class NonTxPerCacheInboundInvocationHandler extends BasePerCacheInboundInvocationHandler {

   @Override
   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      try {
         final boolean onExecutorService = executeOnExecutorService(order, command);
         final BlockingRunnable runnable = createRunnable(command, onExecutorService, reply, order);
         handleRunnable(runnable, onExecutorService);
      } catch (Throwable throwable) {
         reply.reply(exceptionHandlingCommand(command, throwable));
      }
   }

   private BlockingRunnable createRunnable(CacheRpcCommand cmd, boolean onExecutorService, Reply reply, DeliverOrder order) {
      final int commandTopologyId = extractCommandTopologyId(cmd);
      final boolean sync = order.preserveOrder();

      if (cmd instanceof SingleRpcCommand) {
         var topologyMode = onExecutorService ? TopologyMode.READY_TX_DATA : TopologyMode.WAIT_TX_DATA;
         return createDefaultRunnable(cmd, reply, commandTopologyId, topologyMode, sync);
      }

      if (cmd instanceof ConflictResolutionStartCommand ||
            cmd instanceof StateTransferCancelCommand ||
            cmd instanceof StateTransferGetListenersCommand ||
            cmd instanceof StateTransferGetTransactionsCommand ||
            cmd instanceof StateTransferStartCommand) {
         return createDefaultRunnable(cmd, reply, commandTopologyId, false, onExecutorService, sync);
      }
      return createDefaultRunnable(cmd, reply, commandTopologyId, true, onExecutorService, sync);
   }

}
