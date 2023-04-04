package org.infinispan.query.backend;

import static org.infinispan.query.logging.Log.CONTAINER;

import jakarta.transaction.Transaction;
import jakarta.transaction.TransactionManager;

/**
 * Transaction related helper. Wraps a (possibly {@code null}) {@link TransactionManager} and performs transaction
 * suspend and resume on request.
 *
 * @author gustavonalle
 * @since 7.0
 */
public final class TransactionHelper {

   private final TransactionManager transactionManager;

   public TransactionHelper(TransactionManager transactionManager) {
      this.transactionManager = transactionManager;
   }

   public void resume(Transaction transaction) {
      if (transaction != null) {
         try {
            transactionManager.resume(transaction);
         } catch (Exception e) {
            throw CONTAINER.unableToResumeSuspendedTx(transaction, e);
         }
      }
   }

   public Transaction suspendTxIfExists() {
      if (transactionManager == null) {
         return null;
      }
      try {
         return transactionManager.suspend();
      } catch (Exception e) {
         throw CONTAINER.unableToSuspendTx(e);
      }
   }
}
