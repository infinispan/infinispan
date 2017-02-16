package org.infinispan.query.backend;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

import org.infinispan.query.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Transaction related helper
 *
 * @author gustavonalle
 * @since 7.0
 */
public final class TransactionHelper {

   private static final Log LOGGER = LogFactory.getLog(TransactionHelper.class, Log.class);

   private final TransactionManager transactionManager;

   public TransactionHelper(final TransactionManager transactionManager) {
      this.transactionManager = transactionManager;
   }

   public void resume(final Transaction transaction) {
      if (transaction != null) {
         try {
            transactionManager.resume(transaction);
         } catch (Exception e) {
            throw LOGGER.unableToResumeSuspendedTx(transaction, e);
         }
      }
   }

   public Transaction suspendTxIfExists() {
      if (transactionManager == null) {
         return null;
      }
      try {
         Transaction tx;
         if ((tx = transactionManager.getTransaction()) != null) {
            transactionManager.suspend();
         }
         return tx;
      } catch (Exception e) {
         throw LOGGER.unableToSuspendTx(e);
      }
   }

}
