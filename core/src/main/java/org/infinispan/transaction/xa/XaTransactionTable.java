package org.infinispan.transaction.xa;

import java.util.concurrent.ConcurrentMap;

import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.commons.util.CollectionFactory;
import org.infinispan.configuration.cache.Configurations;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
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
   private static final boolean trace = log.isTraceEnabled();

   protected ConcurrentMap<Xid, LocalXaTransaction> xid2LocalTx;
   protected RecoveryManager recoveryManager;
   private String cacheName;

   private boolean onePhaseTotalOrder;

   @Inject
   public void init(RecoveryManager recoveryManager, Cache cache) {
      this.recoveryManager = recoveryManager;
      this.cacheName = cache.getName();
   }

   @Start(priority = 9) // Start before cache loader manager
   @SuppressWarnings("unused")
   public void startXidMapping() {
      //in distributed mode with write skew check, we only allow 2 phases!!
      this.onePhaseTotalOrder = Configurations.isOnePhaseTotalOrderCommit(configuration);

      final int concurrencyLevel = configuration.locking().concurrencyLevel();
      xid2LocalTx = CollectionFactory.makeConcurrentMap(concurrencyLevel, 0.75f, concurrencyLevel);
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
      final Xid xid = localTx.getXid();
      if (xid != null) {
         xid2LocalTx.remove(xid);
      }
   }

   public LocalXaTransaction getLocalTransaction(Xid xid) {
      return this.xid2LocalTx.get(xid);
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
            Xid xid = localTransaction.getXid();
            if (xid != null && !localTransaction.getLookedUpEntries().isEmpty()) {
               log.debug("Attempting a rollback to clear stale resources!");
               try {
                  txCoordinator.rollback(localTransaction);
               } catch (XAException xae) {
                  log.debug("Caught exception attempting to clean up " + xid, xae);
               }
            }
            log.failedToEnlistTransactionXaAdapter(e);
            throw new CacheException(e);
         }
      }
   }

   public int prepare(Xid externalXid) throws XAException {
      Xid xid = convertXid(externalXid);
      LocalXaTransaction localTransaction = getLocalTransactionAndValidate(xid);
      return txCoordinator.prepare(localTransaction);
   }

   public void commit(Xid externalXid, boolean isOnePhase) throws XAException {
      Xid xid = convertXid(externalXid);
      LocalXaTransaction localTransaction = getLocalTransactionAndValidate(xid);
      boolean committedInOnePhase;
      if (isOnePhase && onePhaseTotalOrder) {
         committedInOnePhase = txCoordinator.commit(localTransaction, true);
      } else if (isOnePhase) {
         //isOnePhase being true means that we're the only participant in the distributed transaction and TM does the
         //1PC optimization. We run a 2PC though, as running only 1PC has a high chance of leaving the cluster in
         //inconsistent state.
         txCoordinator.prepare(localTransaction);
         committedInOnePhase = txCoordinator.commit(localTransaction, false);
      } else {
         committedInOnePhase = txCoordinator.commit(localTransaction, false);
      }
      forgetSuccessfullyCompletedTransaction(recoveryManager, localTransaction.getXid(), localTransaction,
            committedInOnePhase);
   }

   void rollback(Xid externalXid) throws XAException {
      Xid xid = convertXid(externalXid);
      LocalXaTransaction localTransaction = getLocalTransactionAndValidate(xid);
      localTransaction.markForRollback(true); //ISPN-879 : make sure that locks are no longer associated to this transactions
      txCoordinator.rollback(localTransaction);
   }

   /**
    * Only does the conversion if recovery is enabled.
    */
   private Xid convertXid(Xid externalXid) {
      if (isRecoveryEnabled()) {
         return XidImpl.copy(externalXid);
      } else {
         return externalXid;
      }
   }

   void start(Xid externalXid, LocalXaTransaction localTransaction) {
      Xid xid = convertXid(externalXid);
      //transform in our internal format in order to be able to serialize
      localTransaction.setXid(xid);
      addLocalTransactionMapping(localTransaction);
      if (trace)
         log.tracef("start called on tx %s", localTransaction.getGlobalTransaction());
   }

   void end(LocalXaTransaction localTransaction) {
      if (trace)
         log.tracef("end called on tx %s(%s)", localTransaction.getGlobalTransaction(), cacheName);
   }

   void forget(Xid externalXid) throws XAException {
      Xid xid = convertXid(externalXid);
      if (trace)
         log.tracef("forget called for xid %s", xid);
      try {
         if (isRecoveryEnabled()) {
            recoveryManager.removeRecoveryInformation(null, xid, true, null, false);
         } else {
            if (trace)
               log.trace("Recovery not enabled");
         }
      } catch (Exception e) {
         log.warnExceptionRemovingRecovery(e);
         XAException xe = new XAException(XAException.XAER_RMERR);
         xe.initCause(e);
         throw xe;
      }
   }

   boolean isRecoveryEnabled() {
      return recoveryManager != null;
   }

   private void forgetSuccessfullyCompletedTransaction(RecoveryManager recoveryManager, Xid xid,
         LocalXaTransaction localTransaction, boolean committedInOnePhase) {
      final GlobalTransaction gtx = localTransaction.getGlobalTransaction();
      if (isRecoveryEnabled()) {
         recoveryManager.removeRecoveryInformation(localTransaction.getRemoteLocksAcquired(), xid, false, gtx,
               partitionHandlingManager.isTransactionPartiallyCommitted(gtx));
         removeLocalTransaction(localTransaction);
      } else {
         releaseLocksForCompletedTransaction(localTransaction, committedInOnePhase);
      }
   }

   private LocalXaTransaction getLocalTransactionAndValidate(Xid xid) throws XAException {
      LocalXaTransaction localTransaction = getLocalTransaction(xid);
      if (localTransaction == null) {
         if (trace)
            log.tracef("no tx found for %s", xid);
         throw new XAException(XAException.XAER_NOTA);
      }
      return localTransaction;
   }
}
