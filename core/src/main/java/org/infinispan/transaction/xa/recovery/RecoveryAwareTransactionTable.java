package org.infinispan.transaction.xa.recovery;

import org.infinispan.commons.CacheException;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.LocalXaTransaction;
import org.infinispan.transaction.xa.XaTransactionTable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.xa.Xid;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Transaction table that delegates prepared transaction's management to the {@link RecoveryManager}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RecoveryAwareTransactionTable extends XaTransactionTable {

   private static final Log log = LogFactory.getLog(RecoveryAwareTransactionTable.class);

   private RecoveryManagerImpl recoveryManager;

   @Inject
   public void initialize(RecoveryManager recoveryManager) {
      this.recoveryManager = (RecoveryManagerImpl) recoveryManager;
   }

   /**
    * Marks the transaction as prepared. If at a further point the originator fails, the transaction is removed form the
    * "normal" transactions collection and moved into the cache that holds in-doubt transactions. See {@link
    * #cleanupStaleTransactions(org.infinispan.topology.CacheTopology)}
    */
   @Override
   public void remoteTransactionPrepared(GlobalTransaction gtx) {
      RecoveryAwareRemoteTransaction remoteTransaction =
            (RecoveryAwareRemoteTransaction) super.getRemoteTransaction(gtx);
      if (remoteTransaction == null)
         throw new CacheException(String.format(
               "Remote transaction for global transaction (%s) not found", gtx));
      remoteTransaction.setPrepared(true);
   }

   /**
    * @see #localTransactionPrepared(org.infinispan.transaction.LocalTransaction)
    */
   @Override
   public void localTransactionPrepared(LocalTransaction localTransaction) {
      ((RecoveryAwareLocalTransaction) localTransaction).setPrepared(true);
   }

   /**
    * First moves the prepared transactions originated on the leavers into the recovery cache and then cleans up the
    * transactions that are not yet prepared.
    * @param cacheTopology
    */
   @Override
   public void cleanupStaleTransactions(CacheTopology cacheTopology) {
      // We only care about transactions originated before this topology update
      if (getMinTopologyId() >= cacheTopology.getTopologyId())
         return;

      Iterator<RemoteTransaction> it = getRemoteTransactions().iterator();
      while (it.hasNext()) {
         RecoveryAwareRemoteTransaction recTx = (RecoveryAwareRemoteTransaction) it.next();
         if (recTx.getTopologyId() < cacheTopology.getTopologyId()) {
            recTx.computeOrphan(cacheTopology.getMembers());
            if (recTx.isInDoubt()) {
               recoveryManager.registerInDoubtTransaction(recTx);
               it.remove();
            }
         }
      }
      //this cleans up the transactions that are not yet prepared
      super.cleanupStaleTransactions(cacheTopology);
   }

   @Override
   public RemoteTransaction getRemoteTransaction(GlobalTransaction txId) {
      RemoteTransaction remoteTransaction = super.getRemoteTransaction(txId);
      if (remoteTransaction != null) return remoteTransaction;
      //also look in the recovery manager, as this transaction might be prepared
      return recoveryManager.getPreparedTransaction(((RecoverableTransactionIdentifier) txId).getXid());
   }

   @Override
   public void remoteTransactionRollback(GlobalTransaction gtx) {
      super.remoteTransactionRollback(gtx);
      recoveryManager.removeRecoveryInformation(((RecoverableTransactionIdentifier) gtx).getXid());
   }

   @Override
   public void remoteTransactionCommitted(GlobalTransaction gtx, boolean onePc) {
      RecoveryAwareRemoteTransaction remoteTransaction = (RecoveryAwareRemoteTransaction) getRemoteTransaction(gtx);
      if (remoteTransaction == null)
         throw new CacheException(String.format("Remote transaction for global transaction (%s) not found", gtx));
      remoteTransaction.markCompleted(true);
      super.remoteTransactionCommitted(gtx, onePc);
   }

   public List<Xid> getLocalPreparedXids() {
      List<Xid> result = new LinkedList<Xid>();
      for (Map.Entry<Xid, LocalXaTransaction> e : xid2LocalTx.entrySet()) {
         RecoveryAwareLocalTransaction value = (RecoveryAwareLocalTransaction) e.getValue();
         if (value.isPrepared()) {
            result.add(e.getKey());
         }
      }
      return result;
   }

   @Override
   public void failureCompletingTransaction(Transaction tx) {
      RecoveryAwareLocalTransaction localTx = (RecoveryAwareLocalTransaction) getLocalTransaction(tx);
      if (localTx == null)
         throw new CacheException(String.format("Local transaction for transaction (%s) not found", tx));

      localTx.setCompletionFailed(true);
      log.tracef("Marked as completion failed %s", localTx);
   }

   public Set<RecoveryAwareLocalTransaction> getLocalTxThatFailedToComplete() {
      Set<RecoveryAwareLocalTransaction> result = new HashSet<RecoveryAwareLocalTransaction>(4);
      for (LocalTransaction lTx : xid2LocalTx.values()) {
         RecoveryAwareLocalTransaction lTx1 = (RecoveryAwareLocalTransaction) lTx;
         if (lTx1.isCompletionFailed()) {
            result.add(lTx1);
         }
      }
      return result;
   }


   /**
    * Iterates over the remote transactions and returns the XID of the one that has an internal id equal with the
    * supplied internal Id.
    */
   public Xid getRemoteTransactionXid(Long internalId) {
      for (RemoteTransaction rTx : getRemoteTransactions()) {
         RecoverableTransactionIdentifier gtx = (RecoverableTransactionIdentifier) rTx.getGlobalTransaction();
         if (gtx.getInternalId() == internalId) {
            if (log.isTraceEnabled()) log.tracef("Found xid %s matching internal id %s", gtx.getXid(), internalId);
            return gtx.getXid();
         }
      }
      log.tracef("Could not find remote transactions matching internal id %s", internalId);
      return null;
   }

   public RemoteTransaction removeRemoteTransaction(Xid xid) {
      if (clustered) {
         Iterator<RemoteTransaction> it = getRemoteTransactions().iterator();
         while (it.hasNext()) {
            RemoteTransaction next = it.next();
            RecoverableTransactionIdentifier gtx = (RecoverableTransactionIdentifier) next.getGlobalTransaction();
            if (xid.equals(gtx.getXid())) {
               it.remove();
               recalculateMinTopologyIdIfNeeded(next);
               next.notifyOnTransactionFinished();
               return next;
            }
         }
      }
      return null;
   }
}
