package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.impl.BaseStateTransferInterceptor;
import org.infinispan.remoting.RemoteException;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Synchronizes the incoming totally ordered transactions with the state transfer.
 *
 * @author Pedro Ruivo
 */
public class TotalOrderStateTransferInterceptor extends BaseStateTransferInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderStateTransferInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   @Override
   public CompletableFuture<Void> visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         return localPrepare(ctx, command);
      }
      return remotePrepare(ctx, command);
   }

   private CompletableFuture<Void> remotePrepare(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      final int topologyId = currentTopologyId();
      ((RemoteTransaction) ctx.getCacheTransaction()).setLookedUpEntriesTopology(command.getTopologyId());

      if (trace) {
         log.tracef("Remote transaction received %s. Tx topology id is %s and current topology is is %s",
                    ctx.getGlobalTransaction().globalId(), command.getTopologyId(), topologyId);
      }

      if (command.getTopologyId() < topologyId) {
         if (log.isDebugEnabled()) {
            log.debugf("Transaction %s delivered in new topology Id. Discard it because it should be retransmitted",
                       ctx.getGlobalTransaction().globalId());
         }
         throw new RetryPrepareException();
      } else if (command.getTopologyId() > topologyId) {
         throw new IllegalStateException("This should never happen");
      }

      return ctx.continueInvocation();
   }

   private CompletableFuture<Void> localPrepare(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      command.setTopologyId(currentTopologyId());

      if (trace) {
         log.tracef("Local transaction received %s. setting topology Id to %s",
               command.getGlobalTransaction().globalId(), command.getTopologyId());
      }

      return ctx.forkInvocation(command,
            (rCtx, rCommand, rv, throwable1) -> handleLocalPrepareReturn(((TxInvocationContext) rCtx),
                  (PrepareCommand) rCommand, rv, throwable1));
   }

   private CompletableFuture<Void> handleLocalPrepareReturn(TxInvocationContext ctx, PrepareCommand command,
         Object rv, Throwable throwable) throws Throwable {
      if (throwable == null)
         return ctx.shortCircuit(rv);

      //if we receive a RetryPrepareException it was because the prepare was delivered during a state
      // transfer.
      //Remember that the REBALANCE_START and CH_UPDATE are totally ordered with the prepares and the
      // prepares are unblocked after the rebalance has finished.
      boolean needsToPrepare = needsToRePrepare(throwable);
      if (log.isDebugEnabled()) {
         log.tracef("Exception caught while preparing transaction %s (cause = %s). Needs to retransmit? %s",
               command.getGlobalTransaction().globalId(), throwable.getCause(), needsToPrepare);
      }

      if (!needsToPrepare) {
         throw throwable;
      } else {
         logRetry(command);
         command.setTopologyId(currentTopologyId());
         return ctx.forkInvocation(command,
               (rCtx, rCommand, rv1, throwable1) -> handleLocalPrepareReturn(((TxInvocationContext) rCtx),
                     (PrepareCommand) rCommand, rv1, throwable1));
      }
   }

   private boolean needsToRePrepare(Throwable throwable) {
      return throwable instanceof RemoteException && throwable.getCause() instanceof RetryPrepareException;
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
