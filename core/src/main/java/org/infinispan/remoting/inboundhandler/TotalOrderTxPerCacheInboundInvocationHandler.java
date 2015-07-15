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
import org.infinispan.interceptors.totalorder.RetryPrepareException;
import org.infinispan.remoting.responses.CacheNotFoundResponse;
import org.infinispan.remoting.responses.ExceptionResponse;
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
   private TotalOrderManager totalOrderManager;

   @Inject
   public void injectTotalOrderManager(TotalOrderManager totalOrderManager) {
      this.totalOrderManager = totalOrderManager;
   }

   @Override
   public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
      try {
         final int commandTopologyId = extractCommandTopologyId(command);
         final boolean onExecutorService;
         final BlockingRunnable runnable;

         switch (command.getCommandId()) {
            case TotalOrderVersionedPrepareCommand.COMMAND_ID:
            case TotalOrderNonVersionedPrepareCommand.COMMAND_ID:
               if (!stateTransferManager.ownsData()) {
                  log.debugf("No Data in local node.");
                  reply.reply(null);
                  return;
               }
               TotalOrderRemoteTransactionState state = ((TotalOrderPrepareCommand) command).getOrCreateState();
               totalOrderManager.ensureOrder(state, ((PrepareCommand) command).getAffectedKeysToLock(false));
               runnable = createRunnableForPrepare(state, (PrepareCommand) command, reply);
               onExecutorService = true;
               break;
            case TotalOrderCommitCommand.COMMAND_ID:
            case TotalOrderVersionedCommitCommand.COMMAND_ID:
            case RollbackCommand.COMMAND_ID:
               onExecutorService = true;
               runnable = createRunnableForCommitOrRollback(command, reply);
               break;
            default:
               onExecutorService = executeOnExecutorService(order, command);
               runnable = createDefaultRunnable(command, reply, commandTopologyId,
                                                command.getCommandId() != StateRequestCommand.COMMAND_ID, onExecutorService);
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
                                                     final Reply reply) {
      return new BaseBlockingRunnable(this, command, reply) {
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
            if (throwable instanceof RetryPrepareException) {
               RetryPrepareException retry = (RetryPrepareException) throwable;
               log.debugf(retry, "Prepare [%s] conflicted with state transfer", command);
               response = new ExceptionResponse(retry);
            }
            log.debugf("Exception received on prepare. Releasing state.");
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

   private BlockingRunnable createRunnableForCommitOrRollback(final CacheRpcCommand command, final Reply reply) {
      return new BaseBlockingRunnable(this, command, reply) {

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
