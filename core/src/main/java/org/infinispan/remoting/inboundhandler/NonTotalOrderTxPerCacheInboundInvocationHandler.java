package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A {@link PerCacheInboundInvocationHandler} implementation for transaction non-total order caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class NonTotalOrderTxPerCacheInboundInvocationHandler extends BasePerCacheInboundInvocationHandler {

   private static final Log log = LogFactory.getLog(NonTotalOrderTxPerCacheInboundInvocationHandler.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      if (order == DeliverOrder.TOTAL) {
         unexpectedDeliverMode(command, order);
      }
      try {
         final int commandTopologyId = extractCommandTopologyId(command);
         final boolean waitTransactionalData = command.getCommandId() != StateRequestCommand.COMMAND_ID;
         final boolean onExecutorService = executeOnExecutorService(order, command);

         handleRunnable(createDefaultRunnable(command, reply, commandTopologyId, waitTransactionalData, onExecutorService), onExecutorService);
      } catch (Throwable throwable) {
         reply.reply(exceptionHandlingCommand(command, throwable));
      }
   }

   @Override
   protected Log getLog() {
      return log;
   }

   @Override
   protected boolean isTraceEnabled() {
      return trace;
   }

}
