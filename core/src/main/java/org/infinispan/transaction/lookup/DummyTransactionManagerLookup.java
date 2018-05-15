package org.infinispan.transaction.lookup;


import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

import org.infinispan.commons.tx.lookup.TransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransactionManager;


/**
 * Returns an instance of {@link org.infinispan.transaction.tm.DummyTransactionManager}.
 *
 * @author Bela Ban Sept 5 2003
 * @since 4.0
 * @deprecated use {@link EmbeddedTransactionManagerLookup}
 */
@Deprecated
public class DummyTransactionManagerLookup implements TransactionManagerLookup {

   @Override
   public TransactionManager getTransactionManager() throws Exception {
      return DummyTransactionManager.getInstance();
   }

   public static UserTransaction getUserTransaction() {
      return DummyTransactionManager.getUserTransaction();
   }

   public static void cleanup() {
      DummyTransactionManager.destroy();
   }

   @Override
   public String toString() {
      return "DummyTransactionManagerLookup";
   }
}
