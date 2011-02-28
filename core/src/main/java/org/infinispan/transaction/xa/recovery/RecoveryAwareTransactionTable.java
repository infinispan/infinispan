package org.infinispan.transaction.xa.recovery;

import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.LocalTransaction;
import org.infinispan.transaction.xa.RemoteTransaction;
import org.infinispan.transaction.xa.TransactionTable;

import javax.transaction.xa.Xid;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Transaction table that delegates prepared transaction's management to the {@link org.infinispan.transaction.xa.recovery.RecoveryManager}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RecoveryAwareTransactionTable extends TransactionTable {

   private RecoveryManagerImpl recoveryManager;

   @Inject
   public void initialize(RecoveryManager recoveryManager) {
      this.recoveryManager = (RecoveryManagerImpl) recoveryManager;
   }

   @Override
   public void remoteTransactionPrepared(GlobalTransaction gtx) {
      RecoveryAwareRemoteTransaction remoteTransaction = (RecoveryAwareRemoteTransaction) remoteTransactions.get(gtx);
      remoteTransaction.setPrepared(true);
      RemoteTransaction preparedTx = remoteTransactions.remove(remoteTransaction.getGlobalTransaction());
      if (preparedTx == null)
         throw new IllegalStateException("This tx has just been prepared, cannot be missing from here!");
      recoveryManager.registerPreparedTransaction(remoteTransaction);
   }

   @Override
   public void localTransactionPrepared(LocalTransaction localTransaction) {
      ((RecoveryAwareLocalTransaction) localTransaction).setPrepared(true);
   }

   @Override
   protected void updateStateOnNodesLeaving(List<Address> leavers) {
      recoveryManager.nodesLeft(leavers);
      super.updateStateOnNodesLeaving(leavers);
   }

   @Override
   public RemoteTransaction getRemoteTransaction(GlobalTransaction txId) {
      RemoteTransaction remoteTransaction = remoteTransactions.get(txId);
      if (remoteTransaction != null) return remoteTransaction;
      //also look in the recovery manager, as this transaction might be prepared
      return recoveryManager.getPreparedTransaction(((XidAware) txId).getXid());
   }

   @Override
   public void remoteTransactionCompleted(GlobalTransaction gtx) {
      recoveryManager.remoteTransactionCompleted(gtx);
   }

   public List<Xid> getLocalPreparedXids() {
      List<Xid> result = new LinkedList<Xid>();
      for (Map.Entry<Xid, LocalTransaction> e : xid2LocalTx.entrySet()) {
         RecoveryAwareLocalTransaction value = (RecoveryAwareLocalTransaction) e.getValue();
         if (value.isPrepared()) {
            result.add(e.getKey());
         }
      }
      return result;
   }
}
