package org.infinispan.transaction.xa;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import jakarta.transaction.Transaction;
import javax.transaction.xa.XAException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * {@link TransactionTable} to be used with {@link TransactionXaAdapter}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class XaTransactionTable extends TransactionTable {
   private static final Log log = LogFactory.getLog(XaTransactionTable.class);

   @Inject protected RecoveryManager recoveryManager;
   @ComponentName(KnownComponentNames.CACHE_NAME)
   @Inject protected String cacheName;

   protected ConcurrentMap<XidImpl, LocalXaTransaction> xid2LocalTx;

   @Start(priority = 9) // Start before cache loader manager
   @SuppressWarnings("unused")
   public void startXidMapping() {
      final int concurrencyLevel = configuration.locking().concurrencyLevel();
      xid2LocalTx = new ConcurrentHashMap<>(concurrencyLevel, 0.75f, concurrencyLevel);
   }

   @Override
   public boolean removeLocalTransaction(LocalTransaction localTx) {
      boolean result = false;
      if (localTx.getTransaction() != null) {//this can be null when we force the invocation during recovery, perhaps on a remote node
         result = super.removeLocalTransaction(localTx);
      }
      removeXidTxMapping((LocalXaTransaction) localTx);
      return result;
   }

   private void removeXidTxMapping(LocalXaTransaction localTx) {
      XidImpl xid = localTx.getXid();
      if (xid != null) {
         xid2LocalTx.remove(xid);
      }
   }

   public LocalXaTransaction getLocalTransaction(XidImpl xid) {
      LocalXaTransaction localTransaction = this.xid2LocalTx.get(xid);
      if (localTransaction == null) {
         if (log.isTraceEnabled())
            log.tracef("no tx found for %s", xid);
      }
      return localTransaction;
   }

   private void addLocalTransactionMapping(LocalXaTransaction localTransaction) {
      if (localTransaction.getXid() == null) throw new IllegalStateException("Initialize xid first!");
      this.xid2LocalTx.put(localTransaction.getXid(), localTransaction);
   }

   @Override
   public void enlist(Transaction transaction, LocalTransaction ltx) {
      LocalXaTransaction localTransaction = (LocalXaTransaction) ltx;
      if (!localTransaction.isEnlisted()) { //make sure that you only enlist it once
         try {
            transaction.enlistResource(new TransactionXaAdapter(localTransaction, this));
         } catch (Exception e) {
            XidImpl xid = localTransaction.getXid();
            if (xid != null && !localTransaction.getLookedUpEntries().isEmpty()) {
               log.debug("Attempting a rollback to clear stale resources asynchronously!");
               txCoordinator.rollback(localTransaction).exceptionally(t -> {
                  log.warn("Caught exception attempting to clean up " + xid + " for " + localTransaction.getGlobalTransaction(), t);
                  return null;
               });
            }
            log.failedToEnlistTransactionXaAdapter(e);
            throw new CacheException(e);
         }
      }
   }

   @Override
   public void enlistClientTransaction(Transaction transaction, LocalTransaction localTransaction) {
      enlist(transaction, localTransaction);
   }

   @Override
   public int getLocalTxCount() {
      return xid2LocalTx.size();
   }

   public CompletionStage<Integer> prepare(XidImpl xid) {
      LocalXaTransaction localTransaction = getLocalTransaction(xid);
      if (localTransaction == null) {
         return CompletableFuture.failedFuture(new XAException(XAException.XAER_NOTA));
      }
      return txCoordinator.prepare(localTransaction);
   }

   public CompletionStage<Void> commit(XidImpl xid, boolean isOnePhase) {
      LocalXaTransaction localTransaction = getLocalTransaction(xid);
      if (localTransaction == null) {
         return CompletableFuture.failedFuture(new XAException(XAException.XAER_NOTA));
      }
      CompletionStage<Boolean> commitStage;

      CompletionStage<Integer> prepareStage;
      //isOnePhase being true means that we're the only participant in the distributed transaction and TM does the
      //1PC optimization. We run a 2PC though, as running only 1PC has a high chance of leaving the cluster in
      //inconsistent state.
      if (isOnePhase && !CompletionStages.isCompletedSuccessfully(prepareStage = txCoordinator.prepare(localTransaction))) {
         commitStage = prepareStage.thenCompose(ignore -> txCoordinator.commit(localTransaction, false));
      } else {
         commitStage = txCoordinator.commit(localTransaction, false);
      }
      if (CompletionStages.isCompletedSuccessfully(commitStage)) {
         boolean committedInOnePhase = CompletionStages.join(commitStage);
         forgetSuccessfullyCompletedTransaction(localTransaction, committedInOnePhase);
         return CompletableFutures.completedNull();
      }
      return commitStage.thenApply(committedInOnePhase -> {
               forgetSuccessfullyCompletedTransaction(localTransaction, committedInOnePhase);
               return null;
            });
   }

   CompletionStage<Void> rollback(XidImpl xid) {
      LocalXaTransaction localTransaction = getLocalTransaction(xid);
      if (localTransaction == null) {
         return CompletableFuture.failedFuture(new XAException(XAException.XAER_NOTA));
      }
      localTransaction.markForRollback(true); //ISPN-879 : make sure that locks are no longer associated to this transactions
      return txCoordinator.rollback(localTransaction);
   }

   void start(XidImpl xid, LocalXaTransaction localTransaction) {
      //transform in our internal format in order to be able to serialize
      localTransaction.setXid(xid);
      addLocalTransactionMapping(localTransaction);
      if (log.isTraceEnabled())
         log.tracef("start called on tx %s", localTransaction.getGlobalTransaction());
   }

   void end(LocalXaTransaction localTransaction) {
      if (log.isTraceEnabled())
         log.tracef("end called on tx %s(%s)", localTransaction.getGlobalTransaction(), cacheName);
   }

   CompletionStage<Void> forget(XidImpl xid) {
      if (log.isTraceEnabled())
         log.tracef("forget called for xid %s", xid);
      if (isRecoveryEnabled()) {
         return recoveryManager.removeRecoveryInformation(null, xid, null, false)
               .exceptionally(t -> {
                  log.warnExceptionRemovingRecovery(t);
                  XAException xe = new XAException(XAException.XAER_RMERR);
                  xe.initCause(t);
                  throw new CompletionException(xe);
               });
      } else {
         if (log.isTraceEnabled())
            log.trace("Recovery not enabled");
      }
      return CompletableFutures.completedNull();
   }

   boolean isRecoveryEnabled() {
      return recoveryManager != null;
   }

   private void forgetSuccessfullyCompletedTransaction(LocalXaTransaction localTransaction, boolean committedInOnePhase) {
      final GlobalTransaction gtx = localTransaction.getGlobalTransaction();
      XidImpl xid = localTransaction.getXid();
      if (isRecoveryEnabled()) {
         recoveryManager.removeRecoveryInformation(localTransaction.getRemoteLocksAcquired(), xid, gtx,
               partitionHandlingManager.isTransactionPartiallyCommitted(gtx));
         removeLocalTransaction(localTransaction);
      } else {
         releaseLocksForCompletedTransaction(localTransaction, committedInOnePhase);
      }
   }
}
