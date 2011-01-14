package org.infinispan.transaction.xa;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

/**
 * This acts both as an local {@link org.infinispan.transaction.xa.CacheTransaction} and implementor of an {@link
 * javax.transaction.xa.XAResource} that will be called by tx manager on various tx stages.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class TransactionXaAdapter implements XAResource {

   private static final Log log = LogFactory.getLog(TransactionXaAdapter.class);
   private static boolean trace = log.isTraceEnabled();

   private int txTimeout;

   private final InvocationContextContainer icc;
   private final InterceptorChain invoker;

   private final CommandsFactory commandsFactory;
   private final Configuration configuration;

   private final TransactionTable txTable;

   /**
    * XAResource is associated with a transaction between enlistment (XAResource.start()) XAResource.end(). It's only the
    * boundary methods (prepare, commit, rollback) that need to be "stateless".
    * Reefer to section 3.4.4 from JTA spec v.1.1
    */
   private final LocalTransaction localTransaction;


   public TransactionXaAdapter(LocalTransaction localTransaction, TransactionTable txTable, CommandsFactory commandsFactory,
                               Configuration configuration, InterceptorChain invoker, InvocationContextContainer icc) {
      this.localTransaction = localTransaction;
      this.txTable = txTable;
      this.commandsFactory = commandsFactory;
      this.configuration = configuration;
      this.invoker = invoker;
      this.icc = icc;
   }

   /**
    * This can be call for any transaction object. See Section 3.4.6 (Resource Sharing) from JTA spec v1.1.
    */
   public int prepare(Xid xid) throws XAException {
      LocalTransaction localTransaction = getLocalTransactionAndValidate(xid);
      
      validateNotMarkedForRollback(localTransaction);

      if (configuration.isOnePhaseCommit()) {
         if (trace) log.trace("Received prepare for tx: {0}. Skipping call as 1PC will be used.", xid);
         return XA_OK;
      }

      PrepareCommand prepareCommand = commandsFactory.buildPrepareCommand(localTransaction.getGlobalTransaction(), localTransaction.getModifications(), configuration.isOnePhaseCommit());
      if (trace) log.trace("Sending prepare command through the chain: " + prepareCommand);

      LocalTxInvocationContext ctx = icc.createTxInvocationContext();
      ctx.setLocalTransaction(localTransaction);
      try {
         invoker.invoke(ctx, prepareCommand);
         if (localTransaction.isReadOnly()) {
            if (trace) log.trace("Readonly transaction: " + localTransaction.getGlobalTransaction());
            // force a cleanup to release any objects held.  Some TMs don't call commit if it is a READ ONLY tx.  See ISPN-845
            commit(xid, false);
            return XA_RDONLY;
         } else {
            return XA_OK;
         }
      } catch (Throwable e) {
         log.error("Error while processing PrepareCommand", e);
         throw new XAException(XAException.XAER_RMERR);
      }
   }

   /**
    * Same comment as for {@link #prepare(javax.transaction.xa.Xid)} applies for commit.
    */
   public void commit(Xid xid, boolean isOnePhase) throws XAException {
      LocalTransaction localTransaction = getLocalTransactionAndValidate(xid);

      if (trace) log.trace("committing transaction {0}" + localTransaction.getGlobalTransaction());
      try {
         LocalTxInvocationContext ctx = icc.createTxInvocationContext();
         ctx.setLocalTransaction(localTransaction);
         if (configuration.isOnePhaseCommit() || isOnePhase) {
            validateNotMarkedForRollback(localTransaction);

            if (trace) log.trace("Doing an 1PC prepare call on the interceptor chain");
            PrepareCommand command = commandsFactory.buildPrepareCommand(localTransaction.getGlobalTransaction(), localTransaction.getModifications(), true);
            try {
               invoker.invoke(ctx, command);
            } catch (Throwable e) {
               log.error("Error while processing 1PC PrepareCommand", e);
               throw new XAException(XAException.XAER_RMERR);
            }
         } else {
            CommitCommand commitCommand = commandsFactory.buildCommitCommand(localTransaction.getGlobalTransaction());
            try {
               invoker.invoke(ctx, commitCommand);
            } catch (Throwable e) {
               log.error("Error while processing 1PC PrepareCommand", e);
               throw new XAException(XAException.XAER_RMERR);
            }
         }
      } finally {
         cleanup(localTransaction);
      }
   }

   /**
    * Same comment as for {@link #prepare(javax.transaction.xa.Xid)} applies for commit.
    */   
   public void rollback(Xid xid) throws XAException {
      TransactionXaAdapter.rollbackImpl(xid, commandsFactory, icc, invoker, txTable);
   }

   public static void rollbackImpl(Xid xid, CommandsFactory commandsFactory, InvocationContextContainer icc, InterceptorChain invoker, TransactionTable txTable) throws XAException {
      LocalTransaction localTransaction = getLocalTransactionAndValidateImpl(xid, txTable);
      if (trace) log.trace("rollback transaction {0} ", localTransaction.getGlobalTransaction());
      RollbackCommand rollbackCommand = commandsFactory.buildRollbackCommand(localTransaction.getGlobalTransaction());
      LocalTxInvocationContext ctx = icc.createTxInvocationContext();
      ctx.setLocalTransaction(localTransaction);
      try {
         invoker.invoke(ctx, rollbackCommand);
      } catch (Throwable e) {
         log.error("Exception while rollback", e);
         throw new XAException(XAException.XA_HEURHAZ);
      } finally {
         cleanupImpl(localTransaction, txTable, icc);
      }
   }

   private LocalTransaction getLocalTransactionAndValidate(Xid xid) throws XAException {
      return TransactionXaAdapter.getLocalTransactionAndValidateImpl(xid, txTable);
   }

   private static LocalTransaction getLocalTransactionAndValidateImpl(Xid xid, TransactionTable txTable) throws XAException {
      LocalTransaction localTransaction = txTable.getLocalTransaction(xid);
      if  (localTransaction == null) {
         if (trace) log.trace("no tx found for {0}", xid);
         throw new XAException(XAException.XAER_NOTA);
      }
      return localTransaction;
   }

   public void start(Xid xid, int i) throws XAException {
      localTransaction.setXid(xid);
      txTable.addLocalTransactionMapping(localTransaction);
      if (trace) log.trace("start called on tx " + this.localTransaction.getGlobalTransaction());
   }

   public void end(Xid xid, int i) throws XAException {
      if (trace) log.trace("end called on tx " + this.localTransaction.getGlobalTransaction());
   }

   public void forget(Xid xid) throws XAException {
      if (trace) log.trace("forget called");
   }

   public int getTransactionTimeout() throws XAException {
      if (trace) log.trace("start called");
      return txTimeout;
   }

   public boolean isSameRM(XAResource xaResource) throws XAException {
      if (!(xaResource instanceof TransactionXaAdapter)) {
         return false;
      }
      TransactionXaAdapter other = (TransactionXaAdapter) xaResource;
      return other.equals(this);
   }

   public Xid[] recover(int i) throws XAException {
      if (trace) log.trace("recover called: " + i);
      return null;
   }

   public boolean setTransactionTimeout(int i) throws XAException {
      this.txTimeout = i;
      return true;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof TransactionXaAdapter)) return false;
      TransactionXaAdapter that = (TransactionXaAdapter) o;
      return this.localTransaction.equals(that.localTransaction);
   }

   @Override
   public int hashCode() {
      return localTransaction.getGlobalTransaction().hashCode();
   }

   @Override
   public String toString() {
      return "TransactionXaAdapter{" +
            "localTransaction=" + localTransaction +
            '}';
   }

   private void validateNotMarkedForRollback(LocalTransaction localTransaction) throws XAException {
      if (localTransaction.isMarkedForRollback()) {
         if (trace) log.trace("Transaction already marked for rollback: {0}", localTransaction);
         throw new XAException(XAException.XA_RBROLLBACK);
      }
   }

   private void cleanup(LocalTransaction localTransaction) {
      TransactionXaAdapter.cleanupImpl(localTransaction, txTable, icc);
   }

   private static void cleanupImpl(LocalTransaction localTransaction, TransactionTable txTable, InvocationContextContainer icc) {
      txTable.removeLocalTransaction(localTransaction);
      icc.suspend();
   }   
}
