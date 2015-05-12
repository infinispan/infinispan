package org.infinispan.transaction.xa.recovery;

import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.transaction.xa.Xid;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * RecoveryManager is the component responsible with managing recovery related information and the functionality
 * associated with it. Refer to <a href="http://community.jboss.org/wiki/Transactionrecoverydesign">this</a> document
 * for details on the design of recovery.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public interface RecoveryManager {

   /**
    * Returns the list of transactions in prepared state from both local and remote cluster nodes. Implementation can
    * take advantage of several optimisations:
    *
    * <ul>
    *    <li>in order to get all tx from the cluster a broadcast is performed. This can be performed only once
    * (assuming the call is successful), the first time this method is called. After that a local, cached list of tx
    * prepared on this node is returned.</li>
    *    <li>during the broadcast just return the list of prepared transactions that are not originated on other active
    * nodes of the cluster.</li>
    * </ul>
    */
   RecoveryIterator getPreparedTransactionsFromCluster();

   /**
    * Returns a {@link Set} containing all the in-doubt transactions from the cluster, including the local node. This
    * does not include transactions that are prepared successfully and for which the originator is still in the
    * cluster.
    *
    * @see InDoubtTxInfo
    */
   Set<InDoubtTxInfo> getInDoubtTransactionInfoFromCluster();

   /**
    * Same as {@link #getInDoubtTransactionInfoFromCluster()}, but only returns transactions from the local node.
    */
   Set<InDoubtTxInfo> getInDoubtTransactionInfo();


   /**
    * Removes from the specified nodes (or all nodes if the value of 'where' is null) the recovery information
    * associated with these Xids.
    *
    * @param skipTxCompletionCommand {@code true} if it must skip the {@link org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand}.
    *                                Used when a partition happens.
    * @param where                   on which nodes should this be executed.
    * @param xid                     the list of xids to be removed.
    * @param sync                    execute sync or async (false)
    * @param gtx                     the global transaction
    * @param fromCluster
    */
   void removeRecoveryInformation(Collection<Address> where, Xid xid, boolean sync, GlobalTransaction gtx, boolean fromCluster);

   /**
    * Same as {@link #removeRecoveryInformation(java.util.Collection, javax.transaction.xa.Xid, boolean,
    * org.infinispan.transaction.xa.GlobalTransaction, boolean)} but the transaction is identified by its internal id,
    * and not by its xid.
    */
   void removeRecoveryInformationFromCluster(Collection<Address> where, long internalId, boolean sync);

   /**
    * Local call that returns a list containing:
    * <pre>
    * - all the remote transactions prepared on this node for which the originator(i.e. the node where the tx
    * stared) is no longer part of the cluster.
    * AND
    * - all the locally originated transactions which are prepared and for which the commit failed
    * </pre>
    *
    * @see RecoveryAwareRemoteTransaction#isInDoubt()
    */
   List<Xid> getInDoubtTransactions();

   /**
    * Local call returning the remote transaction identified by the supplied xid or null.
    */
   RecoveryAwareTransaction getPreparedTransaction(Xid xid);

   /**
    * Replays the given transaction by re-running the prepare and commit. This call expects the transaction to exist on
    * this node either as a local or remote transaction.
    *
    * @param xid    tx to commit or rollback
    * @param commit if true tx is committed, if false it is rolled back
    */
   String forceTransactionCompletion(Xid xid, boolean commit);

   /**
    * This method invokes {@link #forceTransactionCompletion(javax.transaction.xa.Xid, boolean)} on the specified node.
    */
   String forceTransactionCompletionFromCluster(Xid xid, Address where, boolean commit);

   /**
    * Checks both internal state and transaction table's state for the given tx. If it finds it, returns true if tx
    * is prepared.
    */
   boolean isTransactionPrepared(GlobalTransaction globalTx);

   /**
    * Same as {@link #removeRecoveryInformation(javax.transaction.xa.Xid)} but identifies the tx by its internal id.
    */
   RecoveryAwareTransaction removeRecoveryInformation(Long internalId);

   /**
    * Remove recovery information stored on this node (doesn't involve rpc).
    *
    * @param xid@see #removeRecoveryInformation(java.util.Collection, javax.transaction.xa.Xid, boolean)
    */
   RecoveryAwareTransaction removeRecoveryInformation(Xid xid);

   /**
    * Stateful structure allowing prepared-tx retrieval in a batch-oriented manner, as required by {@link
    * javax.transaction.xa.XAResource#recover(int)}.
    */
   interface RecoveryIterator extends Iterator<Xid[]> {

      Xid[] NOTHING = new Xid[]{};

      /**
       * Exhaust the iterator. After this call, {@link #hasNext()} returns false.
       */
      Xid[] all();
   }

   /**
    * An object describing in doubt transaction's state. Needed by the transaction recovery process, for displaying
    * transactions to the user.
    */
   interface InDoubtTxInfo {

      /**
       * Transaction's id.
       */
      Xid getXid();

      /**
       * Each xid has a unique long object associated to it. It makes possible the invocation of recovery operations.
       */
      Long getInternalId();

      /**
       * The value represent transaction's state as described by the {@code status} field. Multiple values are returned
       * as it is possible for an in-doubt transaction to be at the same time e.g. prepared on one node and committed on
       * the other.
       */
      Set<Integer> getStatus();

      /**
       * Returns the set of nodes where this transaction information is maintained.
       */
      Set<Address> getOwners();

      /**
       * Returns true if the transaction information is also present on this node.
       */
      boolean isLocal();
   }
}
