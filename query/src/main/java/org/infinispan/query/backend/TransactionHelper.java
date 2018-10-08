package org.infinispan.query.backend;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Transaction related helper. Wraps a (possibly {@code null}) {@link TransactionManager} and performs transaction
 * suspend and resume on request.
 *
 * @author gustavonalle
 * @since 7.0
 */
public final class TransactionHelper {

   private static final Log log = LogFactory.getLog(TransactionHelper.class, Log.class);

   private final TransactionManager transactionManager;

   public TransactionHelper(TransactionManager transactionManager) {
      this.transactionManager = transactionManager;
   }

   public void resume(Transaction transaction) {
      if (transaction != null) {
         try {
            transactionManager.resume(transaction);
         } catch (Exception e) {
            throw log.unableToResumeSuspendedTx(transaction, e);
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
         throw log.unableToSuspendTx(e);
      }
   }
}
