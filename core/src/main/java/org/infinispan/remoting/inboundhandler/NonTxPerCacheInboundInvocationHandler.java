package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.statetransfer.StateTransferCommand;

/**
 * A {@link org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler} implementation for non-total order
 * caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class NonTxPerCacheInboundInvocationHandler extends BasePerCacheInboundInvocationHandler {

   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      try {
         var onExecutorService = executeOnExecutorService(order, command);
         var waitForTxData = !(command instanceof StateTransferCommand);
         var runnable = createDefaultRunnable(command, reply, extractCommandTopologyId(command), waitForTxData, onExecutorService, order.preserveOrder());
         handleRunnable(runnable, onExecutorService);
      } catch (Throwable throwable) {
         reply.reply(exceptionHandlingCommand(command, throwable));
      }
   }
}
