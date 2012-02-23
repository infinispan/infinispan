package org.infinispan.interceptors.totalorder;

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.VersionedCommitCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.RpcException;
import org.infinispan.totalorder.TotalOrderManager;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.totalOrder.TotalOrderRemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import static org.infinispan.util.Util.prettyPrintGlobalTransaction;

/**
 * Created to control the total order validation. It disable the possibility of acquiring locks during execution through
 * the cache API
 *
 *
 * @author Pedro Ruivo
 * @since 5.2
 */
public class TotalOrderInterceptor extends CommandInterceptor {

   private static final Log log = LogFactory.getLog(TotalOrderInterceptor.class);
   private boolean trace;

   private TotalOrderManager totalOrderManager;

   @Inject
   public void inject(TotalOrderManager totalOrderManager) {
      this.totalOrderManager = totalOrderManager;
   }

   @Start
   public void setLogLevel() {
      this.trace = log.isTraceEnabled();
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      if (trace) {
         log.tracef("Visit Prepare Command. Transaction is %s, Affected keys are %s, Should invoke remotely? %s",
               prettyPrintGlobalTransaction(command.getGlobalTransaction()),
               command.getAffectedKeys(),
               ctx.hasModifications());
      }

      try {
         if(ctx.isOriginLocal()) {
            totalOrderManager.addLocalTransaction(command.getGlobalTransaction(),
                  (LocalTransaction) ctx.getCacheTransaction());
            Object retVal = invokeNextInterceptor(ctx, command);
            return waitForDeliver(ctx, retVal);
         } else {
            totalOrderManager.validateTransaction(command, ctx, getNext());
            return null;
         }
      } catch (Throwable t) {
         if (trace) {
            log.tracef("Exception caught while visiting prepare command. Transaction is %s, Local? %s, " +
                  "version seen are %s, error message is %s",
                  prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                  ctx.isOriginLocal(), ctx.getCacheTransaction().getUpdatedEntryVersions(),
                  t.getMessage());
         }
         throw t;
      }
   }

   @Override
   public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
      throw new UnsupportedOperationException("Lock interface not supported with total order protocol");
   }

   //The rollback and commit command are only invoked with repeatable read + write skew + versioning

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      GlobalTransaction gtx = command.getGlobalTransaction();
      if (trace) {
         log.tracef("Visit Rollback Command. Transaction is %s",
               prettyPrintGlobalTransaction(gtx));
      }

      boolean processCommand = true;

      try {
         if (!ctx.isOriginLocal()) {
            processCommand = totalOrderManager.waitForTxPrepared(
                  (TotalOrderRemoteTransaction) ctx.getCacheTransaction(), false, null);
            if (!processCommand) {
               return null;
            }
         }

         return invokeNextInterceptor(ctx, command);
      } catch (Throwable t) {
         if (trace) {
            log.tracef("Exception caught while visiting local rollback command. Transaction is %s, " +
                  "error message is %s",
                  prettyPrintGlobalTransaction(gtx), t.getMessage());
         }
         throw t;
      } finally {
         if (processCommand) {
            totalOrderManager.finishTransaction(gtx, !ctx.isOriginLocal());
         }
      }
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
      GlobalTransaction gtx = command.getGlobalTransaction();

      if (trace) {
         log.tracef("Visit Commit Command. Transaction is %s",
               prettyPrintGlobalTransaction(gtx));
      }

      boolean processCommand = true;

      try {
         if (!ctx.isOriginLocal()) {
            processCommand = totalOrderManager.waitForTxPrepared(
                  (TotalOrderRemoteTransaction) ctx.getCacheTransaction(), false,
                  command instanceof VersionedCommitCommand ?
                        ((VersionedCommitCommand) command).getUpdatedVersions() :
                        null);
            if (!processCommand) {
               return null;
            }
         }

         return invokeNextInterceptor(ctx, command);
      } catch (Throwable t) {
         if (trace) {
            log.tracef("Exception caught while visiting local commit command. Transaction is %s, " +
                  "version seen are %s, error message is %s",
                  prettyPrintGlobalTransaction(gtx),
                  ctx.getCacheTransaction().getUpdatedEntryVersions(), t.getMessage());
         }
         throw t;
      } finally {
         if (processCommand) {
            totalOrderManager.finishTransaction(gtx, false);
         }
      }
   }

   protected Object waitForDeliver(TxInvocationContext context, Object retVal) {
      //broadcast the command
      boolean sync = configuration.getCacheMode().isSynchronous();

      if(sync) {
         String globalTransactionString = prettyPrintGlobalTransaction(context.getGlobalTransaction());
         //in sync mode, blocks in the LocalTransaction
         if(trace) {
            log.tracef("Transaction [%s] sent in synchronous mode. waiting until modification is applied",
                  globalTransactionString);
         }
         //this is only invoked in local context
         LocalTransaction localTransaction = (LocalTransaction) context.getCacheTransaction();
         try {
            return localTransaction.awaitUntilModificationsApplied();
         } catch (Throwable throwable) {
            throw new RpcException(throwable);
         } finally {
            if(trace) {
               log.tracef("Transaction [%s] finishes the waiting time",
                     globalTransactionString);
            }
         }
      }
      return retVal;
   }
}
