package org.infinispan.transaction.lookup;


import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;


/**
 * Returns an instance of {@link org.infinispan.transaction.tm.EmbeddedTransactionManager}.
 *
 * @author Bela Ban
 * @author Pedro Ruivo
 * @since 9.0
 */
public class EmbeddedTransactionManagerLookup implements TransactionManagerLookup {

   public static UserTransaction getUserTransaction() {
      return EmbeddedTransactionManager.getUserTransaction();
   }

   public static void cleanup() {
      EmbeddedTransactionManager.destroy();
   }

   @Override
   public TransactionManager getTransactionManager() throws Exception {
      return EmbeddedTransactionManager.getInstance();
   }

   @Override
   public String toString() {
      return "EmbeddedTransactionManagerLookup";
   }
}
