package org.infinispan.transaction.xa;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.transaction.xa.recovery.SerializableXid;
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

   private final RecoveryManager recoveryManager;

   private volatile RecoveryManager.RecoveryIterator recoveryIterator;


   public TransactionXaAdapter(LocalTransaction localTransaction, TransactionTable txTable, CommandsFactory commandsFactory,
                               Configuration configuration, InterceptorChain invoker, InvocationContextContainer icc, RecoveryManager rm) {
      this.localTransaction = localTransaction;
      this.txTable = txTable;
      this.commandsFactory = commandsFactory;
      this.configuration = configuration;
      this.invoker = invoker;
      this.icc = icc;
      this.recoveryManager = rm;
   }

   /**
    * This is to be used for creating XAResource objects that should only be used for recovery: i.e. only for calling {@link #recover(int)}.
    * Useful as certain TM implementations (e.g. JBossTM) would use an lookup interface for obtaining a reference to an XAResource implementation.
    * (in the case of JBossTM it is com.arjuna.ats.jta.recovery.XAResourceRecovery).
    */
   public TransactionXaAdapter(TransactionTable txTable) {
      this (null, txTable, null, null, null, null, null);
   }

   /**
    * This can be call for any transaction object. See Section 3.4.6 (Resource Sharing) from JTA spec v1.1.
    */
   public int prepare(Xid externalXid) throws XAException {
      Xid xid = convertXid(externalXid);
      LocalTransaction localTransaction = getLocalTransactionAndValidate(xid);
      
      validateNotMarkedForRollback(localTransaction);

      if (configuration.isOnePhaseCommit()) {
         if (trace) log.trace("Received prepare for tx: %s. Skipping call as 1PC will be used.", xid);
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
            txTable.localTransactionPrepared(localTransaction);
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
   public void commit(Xid externalXid, boolean isOnePhase) throws XAException {
      Xid xid = convertXid(externalXid);
      LocalTransaction localTransaction = getLocalTransactionAndValidate(xid);

      if (trace) log.trace("Committing transaction %s", localTransaction.getGlobalTransaction());
      try {
         LocalTxInvocationContext ctx = icc.createTxInvocationContext();
         ctx.setLocalTransaction(localTransaction);
         if (configuration.isOnePhaseCommit() || isOnePhase) {
            validateNotMarkedForRollback(localTransaction);

            if (trace) log.trace("Doing an 1PC prepare call on the interceptor chain");
            PrepareCommand command = commandsFactory.buildPrepareCommand(localTransaction.getGlobalTransaction(), localTransaction.getModifications(), true);
            try {
               invoker.invoke(ctx, command);
               forgetSuccessfullyCompletedTransaction(recoveryManager, xid, localTransaction);
            } catch (Throwable e) {
               log.error("Error while processing 1PC PrepareCommand", e);
               throw new XAException(XAException.XAER_RMERR);
            }
         } else {
            CommitCommand commitCommand = commandsFactory.buildCommitCommand(localTransaction.getGlobalTransaction());
            try {
               invoker.invoke(ctx, commitCommand);
               forgetSuccessfullyCompletedTransaction(recoveryManager, xid, localTransaction);
            } catch (Throwable e) {
               log.error("Error while processing 1PC PrepareCommand", e);
               throw new XAException(XAException.XAER_RMERR);
            }
         }
      } finally {
         cleanupImpl(localTransaction, txTable, icc);
      }
   }

   /**
    * Same comment as for {@link #prepare(javax.transaction.xa.Xid)} applies for commit.
    */   
   public void rollback(Xid externalXid) throws XAException {
      rollbackImpl(convertXid(externalXid), commandsFactory, icc, invoker, txTable, recoveryManager);
   }

   public static void rollbackImpl(Xid xid, CommandsFactory commandsFactory, InvocationContextContainer icc, InterceptorChain invoker, TransactionTable txTable, RecoveryManager rm) throws XAException {
      LocalTransaction localTransaction = getLocalTransactionAndValidateImpl(xid, txTable);
      if (trace) log.trace("rollback transaction %s ", localTransaction.getGlobalTransaction());
      RollbackCommand rollbackCommand = commandsFactory.buildRollbackCommand(localTransaction.getGlobalTransaction());
      LocalTxInvocationContext ctx = icc.createTxInvocationContext();
      ctx.setLocalTransaction(localTransaction);
      try {
         invoker.invoke(ctx, rollbackCommand);
         forgetSuccessfullyCompletedTransaction(rm, xid, localTransaction);
      } catch (Throwable e) {
         log.error("Exception while rollback", e);
         throw new XAException(XAException.XA_HEURHAZ);
      } finally {
         cleanupImpl(localTransaction, txTable, icc);
      }
   }

   public void start(Xid externalXid, int i) throws XAException {
      Xid xid = convertXid(externalXid);
      //transform in our internal format in order to be able to serialize
      localTransaction.setXid(xid);
      txTable.addLocalTransactionMapping(localTransaction);
      if (trace) log.trace("start called on tx " + this.localTransaction.getGlobalTransaction());
   }

   public void end(Xid externalXid, int i) throws XAException {
      if (trace) log.trace("end called on tx " + this.localTransaction.getGlobalTransaction());
   }

   public void forget(Xid externalXid) throws XAException {
      Xid xid = convertXid(externalXid);
      if (trace) log.trace("forget called for xid %s", xid);
      try {
         recoveryManager.removeRecoveryInformation(null, xid, true);
      } catch (Exception e) {
         log.warn("Exception removing recovery information: ", e);
         throw new XAException(XAException.XAER_RMERR);
      }
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

   public Xid[] recover(int flag) throws XAException {
      if (!configuration.isTransactionRecoveryEnabled()) {
         log.warn("Recovery call will be ignored as recovery is disabled. More on recovery: http://community.jboss.org/docs/DOC-16646");
         return RecoveryManager.RecoveryIterator.NOTHING;
      }
      if (trace) log.trace("recover called: " + flag);

      if (isFlag(flag, TMSTARTRSCAN)) {
         recoveryIterator = recoveryManager.getPreparedTransactionsFromCluster();
         if (trace) log.trace("Fetched a new recovery iterator: %s" , recoveryIterator);
      }
      if (isFlag(flag, TMENDRSCAN)) {
         if (log.isTraceEnabled()) log.trace("Flushing the iterator");
         return recoveryIterator.all();
      } else {
         //as per the spec: "TMNOFLAGS this flag must be used when no other flags are specified."
         if (!isFlag(flag, TMSTARTRSCAN) && !isFlag(flag, TMNOFLAGS))
            throw new IllegalArgumentException("TMNOFLAGS this flag must be used when no other flags are specified." +
                                                     " Received " + flag);
         return recoveryIterator.hasNext() ? recoveryIterator.next() : RecoveryManager.RecoveryIterator.NOTHING;
      }
   }

   private boolean isFlag(int value, int flag) {
      return (value & flag) != 0;
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
         if (trace) log.trace("Transaction already marked for rollback: %s", localTransaction);
         throw new XAException(XAException.XA_RBROLLBACK);
      }
   }

   private static void cleanupImpl(LocalTransaction localTransaction, TransactionTable txTable, InvocationContextContainer icc) {
      txTable.removeLocalTransaction(localTransaction);
      icc.suspend();
   }

   private static void forgetSuccessfullyCompletedTransaction(RecoveryManager recoveryManager, Xid xid, LocalTransaction localTransaction) {
      //todo make sure this gets broad casted or at least flushed
      if (recoveryManager != null) {
         recoveryManager.removeRecoveryInformation(localTransaction.getRemoteLocksAcquired(), xid, false);
      }
   }

   private LocalTransaction getLocalTransactionAndValidate(Xid xid) throws XAException {
      return getLocalTransactionAndValidateImpl(xid, txTable);
   }

   private static LocalTransaction getLocalTransactionAndValidateImpl(Xid xid, TransactionTable txTable) throws XAException {
      LocalTransaction localTransaction = txTable.getLocalTransaction(xid);
      if  (localTransaction == null) {
         if (trace) log.trace("no tx found for %s", xid);
         throw new XAException(XAException.XAER_NOTA);
      }
      return localTransaction;
   }

   public LocalTransaction getLocalTransaction() {
      return localTransaction;
   }

   /**
    * Only does the conversion if recovery is enabled.
    */
   private Xid convertXid(Xid externalXid) {
      if (configuration.isTransactionRecoveryEnabled() && (!(externalXid instanceof SerializableXid))) {
         return new SerializableXid(externalXid);
      } else {
         return externalXid;
      }
   }
}
