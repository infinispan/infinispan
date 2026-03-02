package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.remote.CacheRpcCommand;

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
         var onExecutorService = executeOnExecutorService(order, command);
         var waitForTxData = !noRequireTxData(command);
         var runnable = createDefaultRunnable(command, reply, extractCommandTopologyId(command), waitForTxData, onExecutorService, order.preserveOrder());
         handleRunnable(runnable, onExecutorService);
      } catch (Throwable throwable) {
         reply.reply(exceptionHandlingCommand(command, throwable));
      }
   }
}
