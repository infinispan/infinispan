package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.BasicInvocationStage;
import org.infinispan.interceptors.InvocationComposeHandler;
import org.infinispan.interceptors.InvocationStage;
import org.infinispan.interceptors.impl.BaseStateTransferInterceptor;
import org.infinispan.remoting.RemoteException;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Synchronizes the incoming totally ordered transactions with the state transfer.
 *
 * @author Pedro Ruivo
 */
public class TotalOrderStateTransferInterceptor extends BaseStateTransferInterceptor {
   private static final Log log = LogFactory.getLog(TotalOrderStateTransferInterceptor.class);
   private static final boolean trace = log.isTraceEnabled();

   private final InvocationComposeHandler handleLocalPrepareReturn = this::handleLocalPrepareReturn;

   @Override
   public BasicInvocationStage visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         return localPrepare(ctx, command);
      }
      return remotePrepare(ctx, command);
   }

   private InvocationStage remotePrepare(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
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

      return invokeNext(ctx, command);
   }

   private BasicInvocationStage localPrepare(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      command.setTopologyId(currentTopologyId());

      if (trace) {
         log.tracef("Local transaction received %s. setting topology Id to %s",
               command.getGlobalTransaction().globalId(), command.getTopologyId());
      }

      return invokeNext(ctx, command)
            .compose(handleLocalPrepareReturn);
   }

   private BasicInvocationStage handleLocalPrepareReturn(BasicInvocationStage invocation, InvocationContext ctx,
                                                         VisitableCommand command, Object rv, Throwable t)
         throws Throwable {
      if (t == null) return invocation;

      // If we receive a RetryPrepareException it was because the prepare was delivered during a state transfer.
      // Remember that the REBALANCE_START and CH_UPDATE are totally ordered with the prepares and the
      // prepares are unblocked after the rebalance has finished.
      boolean needsToPrepare = needsToRePrepare(t);
      PrepareCommand prepareCommand = (PrepareCommand) command;
      if (log.isDebugEnabled()) {
         log.tracef("Exception caught while preparing transaction %s (cause = %s). Needs to retransmit? %s",
               prepareCommand.getGlobalTransaction().globalId(), t.getCause(), needsToPrepare);
      }

      if (!needsToPrepare) {
         throw t;
      } else {
         logRetry(command);
         prepareCommand.setTopologyId(currentTopologyId());
         return invokeNext(ctx, command)
               .compose(handleLocalPrepareReturn);
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
