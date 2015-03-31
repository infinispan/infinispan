package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A {@link org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler} implementation for non-total order
 * caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class NonTotalOrderPerCacheInboundInvocationHandler extends BasePerCacheInboundInvocationHandler {

   private static final Log log = LogFactory.getLog(NonTotalOrderPerCacheInboundInvocationHandler.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      if (order == DeliverOrder.TOTAL) {
         unexpectedDeliverMode(command, order);
      }
      try {
         boolean onExecutorService = !order.preserveOrder() && command.canBlock();
         BlockingRunnable runnable;

         switch (command.getCommandId()) {
            case SingleRpcCommand.COMMAND_ID:
               runnable = createDefaultRunnable(command, reply, extractCommandTopologyId((SingleRpcCommand) command),
                                                true, onExecutorService);
               break;
            case MultipleRpcCommand.COMMAND_ID:
               runnable = createDefaultRunnable(command, reply, extractCommandTopologyId((MultipleRpcCommand) command),
                                                true, onExecutorService);
               break;
            case StateRequestCommand.COMMAND_ID:
               // StateRequestCommand is special in that it doesn't need transaction data
               // In fact, waiting for transaction data could cause a deadlock
               runnable = createDefaultRunnable(command, reply,
                     extractCommandTopologyId(((StateRequestCommand) command)), false, onExecutorService);
               break;
            default:
               int commandTopologyId = NO_TOPOLOGY_COMMAND;
               if (command instanceof TopologyAffectedCommand) {
                  commandTopologyId = extractCommandTopologyId((TopologyAffectedCommand) command);
               }
               runnable = createDefaultRunnable(command, reply, commandTopologyId, true, onExecutorService);
               break;
         }
         handleRunnable(runnable, onExecutorService);
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
