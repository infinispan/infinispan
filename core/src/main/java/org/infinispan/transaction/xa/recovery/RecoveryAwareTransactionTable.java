package org.infinispan.transaction.xa.recovery;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import jakarta.transaction.Transaction;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.LocalXaTransaction;
import org.infinispan.transaction.xa.XaTransactionTable;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Transaction table that delegates prepared transaction's management to the {@link RecoveryManager}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RecoveryAwareTransactionTable extends XaTransactionTable {

   private static final Log log = LogFactory.getLog(RecoveryAwareTransactionTable.class);

   /**
    * Marks the transaction as prepared. If at a further point the originator fails, the transaction is removed form the
    * "normal" transactions collection and moved into the cache that holds in-doubt transactions.
    * See {@link #cleanupLeaverTransactions(java.util.List)}
    */
   @Override
   public void remoteTransactionPrepared(GlobalTransaction gtx) {
      RecoveryAwareRemoteTransaction remoteTransaction =
            (RecoveryAwareRemoteTransaction) getRemoteTransaction(gtx);
      if (remoteTransaction == null)
         throw new CacheException(String.format(
               "Remote transaction for global transaction (%s) not found", gtx));
      remoteTransaction.setPrepared(true);
   }

   /**
    * @see #localTransactionPrepared(org.infinispan.transaction.impl.LocalTransaction)
    */
   @Override
   public void localTransactionPrepared(LocalTransaction localTransaction) {
      ((RecoveryAwareLocalTransaction) localTransaction).setPrepared(true);
   }

   /**
    * First moves the prepared transactions originated on the leavers into the recovery cache and then cleans up the
    * transactions that are not yet prepared.
    * @param members The list of cluster members
    */
   @Override
   public void cleanupLeaverTransactions(List<Address> members) {
      Iterator<RemoteTransaction> it = getRemoteTransactions().iterator();
      HashSet<Address> membersSet = new HashSet<>(members); //faster lookup
      while (it.hasNext()) {
         RecoveryAwareRemoteTransaction recTx = (RecoveryAwareRemoteTransaction) it.next();
         if (!transactionOriginatorChecker.isOriginatorMissing(recTx.getGlobalTransaction(), membersSet)) {
            continue; //Hot Rod transaction
         }
         recTx.computeOrphan(membersSet);
         if (recTx.isInDoubt()) {
            recoveryManager.registerInDoubtTransaction(recTx);
            it.remove();
         }
      }
      //this cleans up the transactions that are not yet prepared
      super.cleanupLeaverTransactions(members);
   }

   @Override
   public RemoteTransaction getRemoteTransaction(GlobalTransaction txId) {
      RemoteTransaction remoteTransaction = super.getRemoteTransaction(txId);
      if (remoteTransaction != null) return remoteTransaction;
      //also look in the recovery manager, as this transaction might be prepared
      return (RemoteTransaction) recoveryManager
            .getPreparedTransaction(txId.getXid());
   }

   @Override
   public void remoteTransactionRollback(GlobalTransaction gtx) {
      super.remoteTransactionRollback(gtx);
      recoveryManager.removeRecoveryInformation(gtx.getXid());
   }

   @Override
   public void remoteTransactionCommitted(GlobalTransaction gtx, boolean onePc) {
      RecoveryAwareRemoteTransaction remoteTransaction = (RecoveryAwareRemoteTransaction) getRemoteTransaction(gtx);
      if (remoteTransaction == null)
         throw new CacheException(String.format("Remote transaction for global transaction (%s) not found", gtx));
      remoteTransaction.markCompleted(true);
      super.remoteTransactionCommitted(gtx, onePc);
   }

   public List<XidImpl> getLocalPreparedXids() {
      List<XidImpl> result = new LinkedList<>();
      for (Map.Entry<XidImpl, LocalXaTransaction> e : xid2LocalTx.entrySet()) {
         RecoveryAwareLocalTransaction value = (RecoveryAwareLocalTransaction) e.getValue();
         if (value.isPrepared()) {
            result.add(e.getKey());
         }
      }
      return result;
   }

   @Override
   public void failureCompletingTransaction(Transaction tx) {
      // TODO Change the Transaction parameter to LocalTransaction to avoid the reverse lookup and the
      // NullPointerException when called from RecoveryManagerImpl.forceTransactionCompletion
      RecoveryAwareLocalTransaction localTx = (RecoveryAwareLocalTransaction) getLocalTransaction(tx);
      if (localTx == null)
         throw new CacheException(String.format("Local transaction for transaction (%s) not found", tx));

      localTx.setCompletionFailed(true);
      log.tracef("Marked as completion failed %s", localTx);
   }

   public Set<RecoveryAwareLocalTransaction> getLocalTxThatFailedToComplete() {
      Set<RecoveryAwareLocalTransaction> result = new HashSet<>(4);
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
   public XidImpl getRemoteTransactionXid(Long internalId) {
      for (RemoteTransaction rTx : getRemoteTransactions()) {
         GlobalTransaction gtx = rTx.getGlobalTransaction();
         if (gtx.getInternalId() == internalId) {
            if (log.isTraceEnabled()) log.tracef("Found xid %s matching internal id %s", gtx.getXid(), internalId);
            return gtx.getXid();
         }
      }
      if (log.isTraceEnabled()) log.tracef("Could not find remote transactions matching internal id %s", internalId);
      return null;
   }

   public RemoteTransaction removeRemoteTransaction(XidImpl xid) {
      if (clustered) {
         Iterator<RemoteTransaction> it = getRemoteTransactions().iterator();
         while (it.hasNext()) {
            RemoteTransaction next = it.next();
            GlobalTransaction gtx = next.getGlobalTransaction();
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
