package org.infinispan.query.backend;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;

/**
 * Transaction related helper
 *
 * @author gustavonalle
 * @since 7.0
 */
class TransactionHelper {

   private final TransactionManager transactionManager;

   TransactionHelper(final TransactionManager transactionManager) {
      this.transactionManager = transactionManager;
   }

   void runSuspendingTx(Operation op) {
      final Transaction transaction = suspend();
      try {
         op.execute();
      } finally {
         resume(transaction);
      }
   }

   Transaction suspend() {
      if (transactionManager == null) {
         return null;
      }
      try {
         return transactionManager.suspend();
      } catch (Exception e) {
         //ignored
      }
      return null;
   }

   void resume(Transaction transaction) {
      if (transaction == null || transactionManager == null) {
         return;
      }
      try {
         transactionManager.resume(transaction);
      } catch (Exception e) {
         //ignored;
      }
   }

   static interface Operation {
      void execute();
   }

}
