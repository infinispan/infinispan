package org.infinispan.transaction.xa.recovery;

import jakarta.transaction.Transaction;

import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.LocalXaTransaction;

/**
 * Extends {@link org.infinispan.transaction.xa.LocalXaTransaction} and adds recovery related information.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RecoveryAwareLocalTransaction extends LocalXaTransaction implements RecoveryAwareTransaction {

   private boolean prepared;

   private boolean completionFailed;

   public RecoveryAwareLocalTransaction(Transaction transaction, GlobalTransaction tx, boolean implicitTransaction,
                                        int topologyId, long txCreationTime) {
      super(transaction, tx, implicitTransaction, topologyId, txCreationTime);
   }

   @Override
   public boolean isPrepared() {
      return prepared;
   }

   @Override
   public void setPrepared(boolean prepared) {
      this.prepared = prepared;
   }

   /**
    * Returns true if this transaction failed during 2nd phase of 2PC(prepare or commit). E.g. when the transaction successfully
    * prepared but the commit failed due to a network issue.
    */
   public boolean isCompletionFailed() {
      return completionFailed;
   }

   /**
    * @see  #isCompletionFailed()
    */
   public void setCompletionFailed(boolean completionFailed) {
      this.completionFailed = completionFailed;
   }
}
