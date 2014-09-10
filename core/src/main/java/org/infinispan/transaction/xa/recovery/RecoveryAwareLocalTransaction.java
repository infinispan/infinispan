package org.infinispan.transaction.xa.recovery;

import org.infinispan.commons.equivalence.Equivalence;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.LocalXaTransaction;

import javax.transaction.Transaction;

/**
 * Extends {@link org.infinispan.transaction.xa.LocalXaTransaction} and adds recovery related information.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class RecoveryAwareLocalTransaction extends LocalXaTransaction implements RecoveryAwareTransaction {

   private boolean prepared;

   private boolean completionFailed;

   public RecoveryAwareLocalTransaction(Transaction transaction, GlobalTransaction tx,
         boolean implicitTransaction, int topologyId, Equivalence<Object> keyEquivalence, long txCreationTime) {
      super(transaction, tx, implicitTransaction, topologyId, keyEquivalence, txCreationTime);
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
