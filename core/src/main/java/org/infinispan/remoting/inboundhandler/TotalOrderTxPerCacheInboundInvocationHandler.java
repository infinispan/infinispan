package org.infinispan.remoting.inboundhandler;

import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderCommitCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderNonVersionedPrepareCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderPrepareCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderVersionedCommitCommand;
import org.infinispan.commands.tx.totalorder.TotalOrderVersionedPrepareCommand;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.statetransfer.OutdatedTopologyException;
import org.infinispan.statetransfer.StateConsumer;
import org.infinispan.statetransfer.StateRequestCommand;
import org.infinispan.transaction.impl.TotalOrderRemoteTransactionState;
import org.infinispan.transaction.totalorder.TotalOrderLatch;
import org.infinispan.transaction.totalorder.TotalOrderManager;
import org.infinispan.util.concurrent.BlockingRunnable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * A {@link org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler} implementation for total order
 * caches.
 *
 * @author Pedro Ruivo
 * @since 7.1
 */
public class TotalOrderTxPerCacheInboundInvocationHandler extends BasePerCacheInboundInvocationHandler {

   private static final Log log = LogFactory.getLog(TotalOrderTxPerCacheInboundInvocationHandler.class);
   private static final boolean trace = log.isTraceEnabled();
   @Inject private TotalOrderManager totalOrderManager;
   @Inject private StateConsumer stateConsumer;

   @Override
   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      try {
         final int commandTopologyId = extractCommandTopologyId(command);
         if (isCommandSentBeforeFirstTopology(commandTopologyId)) {
            reply.reply(CacheNotFoundResponse.INSTANCE);
            return;
         }

         final boolean onExecutorService;
         final boolean sync = order.preserveOrder();
         final BlockingRunnable runnable;
         switch (command.getCommandId()) {
            case TotalOrderVersionedPrepareCommand.COMMAND_ID:
            case TotalOrderNonVersionedPrepareCommand.COMMAND_ID:
               if (!stateConsumer.ownsData()) {
                  log.debugf("No Data in local node.");
                  reply.reply(null);
                  return;
               }
               TotalOrderRemoteTransactionState state = ((TotalOrderPrepareCommand) command).getOrCreateState();
               totalOrderManager.ensureOrder(state, ((PrepareCommand) command).getKeysToLock());
               runnable = createRunnableForPrepare(state, (PrepareCommand) command, reply, sync);
               onExecutorService = true;
               break;
            case TotalOrderCommitCommand.COMMAND_ID:
            case TotalOrderVersionedCommitCommand.COMMAND_ID:
            case RollbackCommand.COMMAND_ID:
               onExecutorService = true;
               runnable = createRunnableForCommitOrRollback(command, reply, sync);
               break;
            default:
               onExecutorService = executeOnExecutorService(order, command);
               runnable = createDefaultRunnable(command, reply, commandTopologyId,
                     command.getCommandId() != StateRequestCommand.COMMAND_ID, onExecutorService, sync);
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

   private BlockingRunnable createRunnableForPrepare(final TotalOrderRemoteTransactionState state,
                                                     final PrepareCommand command,
                                                     final Reply reply, boolean sync) {
      return new BaseBlockingRunnable(this, command, reply, sync) {
         @Override
         public boolean isReady() {
            for (TotalOrderLatch block : state.getConflictingTransactionBlocks()) {
               if (block.isBlocked()) {
                  return false;
               }
            }
            return true;
         }

         @Override
         protected void onException(Throwable throwable) {
            if (throwable instanceof OutdatedTopologyException) {
               if (trace)
                  log.tracef(throwable, "Prepare [%s] conflicted with state transfer. Releasing state and retrying.", command);
            } else {
               log.debugf("Exception received on prepare. Releasing state.");
            }
            totalOrderManager.release(state);
         }

         @Override
         protected void afterInvoke() {
            if (response instanceof ExceptionResponse) {
               if (trace) {
                  log.trace("Exception received on prepare. Releasing state.");
               }
               totalOrderManager.release(state);
            }
         }

         @Override
         protected void onFinally() {
            //invoked after the reply is sent!
            if (((PrepareCommand) command).isOnePhaseCommit() || response instanceof ExceptionResponse) {
               remoteCommandsExecutor.checkForReadyTasks();
            }
         }
      };
   }

   private BlockingRunnable createRunnableForCommitOrRollback(final CacheRpcCommand command, final Reply reply,
                                                              boolean sync) {
      return new BaseBlockingRunnable(this, command, reply, sync) {

         @Override
         public boolean isReady() {
            return true;
         }

         @Override
         protected void onFinally() {
            remoteCommandsExecutor.checkForReadyTasks();
         }
      };
   }
}
