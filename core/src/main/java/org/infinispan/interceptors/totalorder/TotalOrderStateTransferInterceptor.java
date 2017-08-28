package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.TopologyAffectedCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.InvocationExceptionFunction;
import org.infinispan.interceptors.impl.BaseStateTransferInterceptor;
import org.infinispan.statetransfer.OutdatedTopologyException;
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

   private final InvocationExceptionFunction handleLocalPrepareReturn = this::handleLocalPrepareReturn;

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         return localPrepare(ctx, command);
      }
      return remotePrepare(ctx, command);
   }

   private Object remotePrepare(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
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
         throw OutdatedTopologyException.INSTANCE;
      } else if (command.getTopologyId() > topologyId) {
         throw new IllegalStateException("This should never happen");
      }

      return invokeNext(ctx, command);
   }

   private Object localPrepare(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      command.setTopologyId(currentTopologyId());

      if (trace) {
         log.tracef("Local transaction received %s. setting topology Id to %s",
               command.getGlobalTransaction().globalId(), command.getTopologyId());
      }

      return invokeNextAndExceptionally(ctx, command, handleLocalPrepareReturn);
   }

   private Object handleLocalPrepareReturn(InvocationContext ctx, VisitableCommand command, Throwable t)
         throws Throwable {
      assert t != null;
      // If we receive a RetryPrepareException it was because the prepare was delivered during a state transfer.
      // Remember that the REBALANCE_START and CH_UPDATE are totally ordered with the prepares and the
      // prepares are unblocked after the rebalance has finished.
      boolean needsToPrepare = needsToRePrepare(t);
      PrepareCommand prepareCommand = (PrepareCommand) command;
      if (trace) {
         log.tracef("Exception caught while preparing transaction %s (cause = %s). Needs to retransmit? %s",
               prepareCommand.getGlobalTransaction().globalId(), t.getCause(), needsToPrepare);
      }

      if (!needsToPrepare) {
         throw t;
      } else {
         int newTopologyId = currentTopologyId();
         logRetry(newTopologyId, (TopologyAffectedCommand) command);
         prepareCommand.setTopologyId(newTopologyId);
         return invokeNextAndExceptionally(ctx, command, handleLocalPrepareReturn);
      }
   }

   private boolean needsToRePrepare(Throwable throwable) {
      return throwable instanceof OutdatedTopologyException;
   }

   @Override
   protected Log getLog() {
      return log;
   }
}
