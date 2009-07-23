package org.infinispan.interceptors;

import org.infinispan.commands.DataCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.DeadlockDetectingGlobalTransaction;
import org.infinispan.transaction.xa.TransactionTable;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.concurrent.locks.LockManager;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Set;

/**
 * This interceptor populates the {@link org.infinispan.transaction.xa.DeadlockDetectingGlobalTransaction} with
 * appropriate information needed in order to accomplish deadlock detection. It MUST process populate data before the
 * replication takes place, so it will do all the tasks before calling {@link org.infinispan.interceptors.base.CommandInterceptor#invokeNextInterceptor(org.infinispan.context.InvocationContext,
 * org.infinispan.commands.VisitableCommand)}.
 * <p/>
 * Note: for local caches, deadlock detection dos NOT work for aggregate operations (clear, putAll).
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class DeadlockDetectingInterceptor extends CommandInterceptor {

   private TransactionTable txTable;
   private LockManager lockManager;
   private TransactionManager txManager;

   @Inject
   public void init(TransactionTable txTable, LockManager lockManager, TransactionManager txManager) {
      this.txTable = txTable;
      this.lockManager = lockManager;
      this.txManager = txManager;
   }


   /**
    * Only does a sanity check.
    */
   @Start
   public void start() {
      if (!configuration.isEnableDeadlockDetection()) {
         throw new IllegalStateException("This interceptor should not be present in the chain as deadlock detection is not used!");
      }
   }

   private Object handleDataCommand(InvocationContext ctx, DataCommand command) throws Throwable {
      if (ctx.isInTxScope()) {
         DeadlockDetectingGlobalTransaction gtx = (DeadlockDetectingGlobalTransaction) ctx.getLockOwner();
         gtx.setLockInterntion(command.getKey());
         gtx.setProcessingThread(Thread.currentThread());
      }
      try {
         return invokeNextInterceptor(ctx, command);
      } catch (InterruptedException ie) {
         if (ctx.isInTxScope()) {
            lockManager.releaseLocks(ctx);
            if (ctx.isOriginLocal()) {
               Transaction transaction = txManager.getTransaction();
               if (trace)
                  log.trace("Marking the transaction for rollback! : " + transaction);
               if (transaction == null) {
                  throw new IllegalStateException("We're running in a local transaction, there MUST be one " +
                        "associated witht the local thread but none found! " + transaction);
               }
               transaction.setRollbackOnly();
               txTable.removeLocalTransaction(transaction);
               throw new DeadlockDetectedException("Deadlock request was detected for locally originated tx " + transaction +
                     "; it was marked for rollback");
            } else {
               DeadlockDetectingGlobalTransaction gtx = (DeadlockDetectingGlobalTransaction) ctx.getLockOwner();
               gtx.setMarkedForRollback(true);
               throw new DeadlockDetectedException("Deadlock request was detected for remotely originated tx " + gtx +
                     "; it was marked for rollback");
            }
         } else {
            if (trace)
               log.trace("Received an interrupt request, but we're not running within the scope of a transaction, so passing it up the stack", ie);
            throw ie;
         }
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      return handleDataCommand(ctx, command);
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
      DeadlockDetectingGlobalTransaction globalTransaction = (DeadlockDetectingGlobalTransaction) ctx.getGlobalTransaction();
      globalTransaction.setProcessingThread(Thread.currentThread());
      if (ctx.isOriginLocal()) {
         if (configuration.getCacheMode().isDistributed()) {
            Set<Address> transactionParticipants = ctx.getTransactionParticipants();
            globalTransaction.setReplicatingTo(transactionParticipants);
         } else {
            globalTransaction.setReplicatingTo(null);
         }
         if (trace) log.trace("Deadlock detection information was added to " + globalTransaction);
      }
      try {
         return invokeNextInterceptor(ctx, command);
      } catch (Throwable dde) {
         if (ctx.isOriginLocal()) {
            globalTransaction.setMarkedForRollback(true);
            boolean wasInterrupted = Thread.interrupted();
            if (trace)
               log.trace("Deadlock was detected on the remote side, marking the tx for rollback. Was this thread interrupted? " + wasInterrupted);
         }
         throw dde;
      } finally {
         if (!ctx.isOriginLocal()) {
            if (!txTable.containRemoteTx(ctx.getGlobalTransaction())) {
               if (trace) {
                  log.trace("While returning from prepare we determined that remote tx is no longer in the txTable. " +
                        "This means that a rollback was executed in between; releasing locks");
               }
               lockManager.releaseLocks(ctx);
            }
         }
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
      if (!ctx.isOriginLocal()) {
         DeadlockDetectingGlobalTransaction globalTransaction = (DeadlockDetectingGlobalTransaction) ctx.getGlobalTransaction();
         globalTransaction.interruptProcessingThread();
      }
      return invokeNextInterceptor(ctx, command);
   }
}
