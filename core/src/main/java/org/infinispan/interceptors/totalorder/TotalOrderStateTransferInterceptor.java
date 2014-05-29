package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.BaseStateTransferInterceptor;
import org.infinispan.remoting.RemoteException;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Synchronizes the incoming totally ordered transactions with the state transfer.
 *
 * @author Pedro Ruivo
 * @since 5.3
 */
public class TotalOrderStateTransferInterceptor extends BaseStateTransferInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderStateTransferInterceptor.class);

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (ctx.isOriginLocal()) {
         return localPrepare(ctx, command);
      }
      return remotePrepare(ctx, command);
   }

   @Override
   protected Log getLog() {
      return log;
   }

   private Object remotePrepare(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      final int topologyId = currentTopologyId();
      ((RemoteTransaction) ctx.getCacheTransaction()).setLookedUpEntriesTopology(command.getTopologyId());

      if (log.isTraceEnabled()) {
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

      return invokeNextInterceptor(ctx, command);
   }

   private Object localPrepare(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      boolean needsToPrepare = true;
      Object retVal = null;
      while (needsToPrepare) {
         try {
            command.setTopologyId(currentTopologyId());

            if (log.isTraceEnabled()) {
               log.tracef("Local transaction received %s. setting topology Id to %s",
                          command.getGlobalTransaction().globalId(), command.getTopologyId());
            }

            retVal = invokeNextInterceptor(ctx, command);
            needsToPrepare = false;
         } catch (Throwable throwable) {
            //if we receive a RetryPrepareException it was because the prepare was delivered during a state transfer.
            //Remember that the REBALANCE_START and CH_UPDATE are totally ordered with the prepares and the prepares are
            //unblocked after the rebalance has finished.
            needsToPrepare = needsToRePrepare(throwable);
            if (log.isDebugEnabled()) {
               log.tracef("Exception caught while preparing transaction %s (cause = %s). Needs to retransmit? %s",
                          command.getGlobalTransaction().globalId(), throwable.getCause(), needsToPrepare);
            }

            if (!needsToPrepare) {
               throw throwable;
            } else {
               logRetry(command);
            }
         }
      }
      return retVal;
   }

   private boolean needsToRePrepare(Throwable throwable) {
      return throwable instanceof RemoteException && throwable.getCause() instanceof RetryPrepareException;
   }
}
