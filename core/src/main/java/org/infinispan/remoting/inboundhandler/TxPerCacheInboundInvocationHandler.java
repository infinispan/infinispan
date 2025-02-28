package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.statetransfer.StateTransferCommand;
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
         if (command instanceof StateTransferCommand) {
            runnable = createDefaultRunnable(command, reply, commandTopologyId, false, onExecutorService, sync);
         } else {
            runnable = createDefaultRunnable(command, reply, commandTopologyId, true, onExecutorService, sync);
         }
         handleRunnable(runnable, onExecutorService);
      } catch (Throwable throwable) {
         reply.reply(exceptionHandlingCommand(command, throwable));
      }
   }
}
