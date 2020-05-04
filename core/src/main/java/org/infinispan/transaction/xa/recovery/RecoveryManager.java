package org.infinispan.transaction.xa.recovery;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;

import org.infinispan.commons.tx.XidImpl;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;

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
    * @param where       on which nodes should this be executed.
    * @param xid         the list of xids to be removed.
    * @param gtx         the global transaction
    * @param fromCluster {@code true} to remove the recovery information from all cluster.
    */
   CompletionStage<Void> removeRecoveryInformation(Collection<Address> where, XidImpl xid, GlobalTransaction gtx,
         boolean fromCluster);

   /**
    * Same as {@link #removeRecoveryInformation(Collection, XidImpl, GlobalTransaction, boolean)}
    * but the transaction is identified by its internal id, and not by its xid.
    */
   CompletionStage<Void> removeRecoveryInformationFromCluster(Collection<Address> where, long internalId);

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
   List<XidImpl> getInDoubtTransactions();

   /**
    * Local call returning the remote transaction identified by the supplied xid or null.
    */
   RecoveryAwareTransaction getPreparedTransaction(XidImpl xid);

   /**
    * Replays the given transaction by re-running the prepare and commit. This call expects the transaction to exist on
    * this node either as a local or remote transaction.
    *
    * @param xid    tx to commit or rollback
    * @param commit if true tx is committed, if false it is rolled back
    */
   CompletionStage<String> forceTransactionCompletion(XidImpl xid, boolean commit);

   /**
    * This method invokes {@link #forceTransactionCompletion(XidImpl, boolean)} on the specified node.
    */
   String forceTransactionCompletionFromCluster(XidImpl xid, Address where, boolean commit);

   /**
    * Checks both internal state and transaction table's state for the given tx. If it finds it, returns true if tx
    * is prepared.
    */
   boolean isTransactionPrepared(GlobalTransaction globalTx);

   /**
    * Same as {@link #removeRecoveryInformation(XidImpl)} but identifies the tx by its internal id.
    */
   RecoveryAwareTransaction removeRecoveryInformation(Long internalId);

   /**
    * Remove recovery information stored on this node (doesn't involve rpc).
    *
    * @see #removeRecoveryInformation(Collection, XidImpl, GlobalTransaction, boolean)
    */
   RecoveryAwareTransaction removeRecoveryInformation(XidImpl xid);

   void registerInDoubtTransaction(RecoveryAwareRemoteTransaction tx);

   /**
    * Stateful structure allowing prepared-tx retrieval in a batch-oriented manner, as required by {@link
    * javax.transaction.xa.XAResource#recover(int)}.
    */
   interface RecoveryIterator extends Iterator<XidImpl[]> {

      XidImpl[] NOTHING = new XidImpl[]{};

      /**
       * Exhaust the iterator. After this call, {@link #hasNext()} returns false.
       */
      XidImpl[] all();
   }
}
