package org.infinispan.transaction.xa;

import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.infinispan.commons.tx.AsyncXaResource;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.transaction.impl.AbstractEnlistmentAdapter;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * This acts both as an local {@link org.infinispan.transaction.xa.CacheTransaction} and implementor of an {@link
 * javax.transaction.xa.XAResource} that will be called by tx manager on various tx stages.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class TransactionXaAdapter extends AbstractEnlistmentAdapter implements XAResource, AsyncXaResource {

   private static final Log log = LogFactory.getLog(TransactionXaAdapter.class);

   /**
    * It is really useful only if TM and client are in separate processes and TM fails. This is because a client might
    * call tm.begin and then the TM (running separate process) crashes. In this scenario the TM won't ever call
    * XAResource.rollback, so these resources would be held there forever. By knowing the timeout the RM can proceed
    * releasing the resources associated with given tx.
    */
   private int txTimeout;

   private final XaTransactionTable txTable;


   /**
    * XAResource is associated with a transaction between enlistment (XAResource.start()) XAResource.end(). It's only the
    * boundary methods (prepare, commit, rollback) that need to be "stateless".
    * Reefer to section 3.4.4 from JTA spec v.1.1
    */
   private final LocalXaTransaction localTransaction;
   private volatile RecoveryManager.RecoveryIterator recoveryIterator;

   public TransactionXaAdapter(LocalXaTransaction localTransaction, XaTransactionTable txTable) {
      super(localTransaction);
      this.txTable = txTable;
      this.localTransaction = localTransaction;
   }

   public TransactionXaAdapter(XaTransactionTable txTable) {
      super();
      this.txTable = txTable;
      localTransaction = null;
   }

   /**
    * This can be call for any transaction object. See Section 3.4.6 (Resource Sharing) from JTA spec v1.1.
    */
   @Override
   public int prepare(Xid externalXid) throws XAException {
      return runRethrowingXAException(txTable.prepare(XidImpl.copy(externalXid)));
   }

   /**
    * Same comment as for {@link #prepare(javax.transaction.xa.Xid)} applies for commit.
    */
   @Override
   public void commit(Xid externalXid, boolean isOnePhase) throws XAException {
      runRethrowingXAException(txTable.commit(XidImpl.copy(externalXid), isOnePhase));
   }

   /**
    * Same comment as for {@link #prepare(javax.transaction.xa.Xid)} applies for commit.
    */
   @Override
   public void rollback(Xid externalXid) throws XAException {
      runRethrowingXAException(txTable.rollback(XidImpl.copy(externalXid)));
   }

   @Override
   public void start(Xid externalXid, int i) throws XAException {
      assert localTransaction != null;
      txTable.start(XidImpl.copy(externalXid), localTransaction);
   }

   @Override
   public void end(Xid externalXid, int i) {
      txTable.end(this.localTransaction);
   }

   @Override
   public void forget(Xid externalXid) throws XAException {
      runRethrowingXAException(txTable.forget(XidImpl.copy(externalXid)));
   }

   @Override
   public int getTransactionTimeout() {
      if (log.isTraceEnabled()) log.trace("start called");
      return txTimeout;
   }

   /**
    * the only situation in which it returns true is when the other xa resource pertains to the same cache, on
    * the same node.
    */
   @Override
   public boolean isSameRM(XAResource xaResource) {
      return isIsSameRM(xaResource);
   }

   private boolean isIsSameRM(XAResource xaResource) {
      if (!(xaResource instanceof TransactionXaAdapter)) {
         return false;
      }
      TransactionXaAdapter other = (TransactionXaAdapter) xaResource;
      //there is only one enlistment manager per cache and this is more efficient that equals.
      return this.txTable == other.txTable;
   }

   @Override
   public Xid[] recover(int flag) {
      if (!txTable.isRecoveryEnabled()) {
         log.recoveryIgnored();
         return RecoveryManager.RecoveryIterator.NOTHING;
      }
      if (log.isTraceEnabled())
         log.trace("recover called: " + flag);

      if (isFlag(flag, TMSTARTRSCAN)) {
         recoveryIterator = txTable.recoveryManager.getPreparedTransactionsFromCluster();
         if (log.isTraceEnabled())
            log.tracef("Fetched a new recovery iterator: %s", recoveryIterator);
      }
      if (isFlag(flag, TMENDRSCAN)) {
         if (log.isTraceEnabled())
            log.trace("Flushing the iterator");
         return recoveryIterator.all();
      } else {
         //as per the spec: "TMNOFLAGS this flag must be used when no other flags are specified."
         if (!isFlag(flag, TMSTARTRSCAN) && !isFlag(flag, TMNOFLAGS))
            throw new IllegalArgumentException(
                  "TMNOFLAGS this flag must be used when no other flags are specified." +
                        " Received " + flag);
         return recoveryIterator.hasNext() ? recoveryIterator.next() : RecoveryManager.RecoveryIterator.NOTHING;

      }
   }

   @Override
   public boolean setTransactionTimeout(int i) {
      this.txTimeout = i;
      return true;
   }

   @Override
   public String toString() {
      return "TransactionXaAdapter{" +
            "localTransaction=" + localTransaction +
            '}';
   }

   public LocalXaTransaction getLocalTransaction() {
      return localTransaction;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TransactionXaAdapter that = (TransactionXaAdapter) o;

      //also include the enlistment manager in comparison - needed when same tx spans multiple caches.
      return Objects.equals(localTransaction, that.localTransaction) &&
            txTable == that.txTable;
   }

   private static boolean isFlag(int value, int flag) {
      return (value & flag) != 0;
   }

   private static <T> T runRethrowingXAException(CompletionStage<T> completionStage) throws XAException {
      try {
         return CompletionStages.join(completionStage);
      } catch (CompletionException e) {
         Throwable cause = e.getCause();
         if (cause instanceof XAException) {
            throw (XAException) cause;
         }
         throw e;
      }
   }

   @Override
   public CompletionStage<Void> asyncEnd(XidImpl xid, int flags) {
      txTable.end(localTransaction);
      return CompletableFutures.completedNull();
   }

   @Override
   public CompletionStage<Integer> asyncPrepare(XidImpl xid) {
      return txTable.prepare(xid);
   }

   @Override
   public CompletionStage<Void> asyncCommit(XidImpl xid, boolean onePhase) {
      return txTable.commit(xid, onePhase);
   }

   @Override
   public CompletionStage<Void> asyncRollback(XidImpl xid) {
      return txTable.rollback(xid);
   }
}
