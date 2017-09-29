package org.infinispan.server.hotrod.tx;

import java.util.List;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.tm.EmbeddedTransaction;

/**
 * A decode context to handle prepare request from a client.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public class PrepareTransactionDecodeContext extends TransactionDecodeContext {

   private EmbeddedTransaction tx;

   public PrepareTransactionDecodeContext(AdvancedCache<byte[], byte[]> cache, XidImpl xid) {
      super(cache, xid);
   }

   /**
    * Starts a transaction.
    *
    * @return {@code true} if the transaction can be started, {@code false} otherwise.
    */
   public boolean startTransaction() {
      try {
         tm.begin();
      } catch (NotSupportedException | SystemException e) {
         return false;
      }
      EmbeddedTransaction tx = tm.getTransaction();
      tx.setXid(xid);
      LocalTransaction localTransaction = transactionTable.getOrCreateLocalTransaction(tx, false);
      TxState txState = new TxState(localTransaction.getGlobalTransaction());
      if (!serverTransactionTable.addGlobalState(xid, txState)) {
         transactionTable.removeLocalTransaction(localTransaction);
         try {
            tm.rollback();
         } catch (SystemException e) {
            //no-op
         }
         return false;
      } else {
         this.txState = txState;
         this.tx = tx;
         serverTransactionTable.createLocalTx(xid, tx);
         transactionTable.enlist(tx, localTransaction);
         return true;
      }
   }

   /**
    * Rollbacks the transaction if it fails to prepare.
    */
   public int rollback() {
      advance(txState.rollback());
      try {
         tx.runCommit(true);
      } catch (HeuristicMixedException e) {
         return XAException.XA_HEURMIX;
      } catch (HeuristicRollbackException e) {
         return XAException.XA_HEURRB;
      } catch (RollbackException e) {
         return XAException.XA_RBROLLBACK;
      }
      return XAException.XA_RBROLLBACK;
   }

   /**
    * Prepares the transaction.
    *
    * @param onePhaseCommit {@code true} if one phase commit.
    * @return the {@link javax.transaction.xa.XAResource#XA_OK} if successful prepared, otherwise one of the {@link javax.transaction.xa.XAException} error codes.
    */
   public int prepare(boolean onePhaseCommit) {
      if (tx.runPrepare()) {
         prepared();
         if (onePhaseCommit) {
            return commit();
         }
         return XAResource.XA_OK;
      } else {
         return rollback();
      }
   }

   public void setRollbackOnly() throws SystemException {
      tx.setRollbackOnly();
   }

   private int commit() {
      advance(txState.commit());
      try {
         tx.runCommit(false);
      } catch (HeuristicMixedException e) {
         return XAException.XA_HEURMIX;
      } catch (HeuristicRollbackException e) {
         return XAException.XA_HEURRB;
      } catch (RollbackException e) {
         return XAException.XA_RBROLLBACK;
      }
      return XAResource.XA_OK;
   }

   /**
    * Advances the transaction to prepared state.
    */
   private void prepared() {
      List<WriteCommand> modifications = transactionTable.getLocalTransaction(tx).getModifications();
      advance(txState.prepare(modifications));
   }
}
