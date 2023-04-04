package org.infinispan.hotrod.transaction.lookup;

import jakarta.transaction.TransactionManager;

import org.infinispan.hotrod.transaction.manager.RemoteTransactionManager;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;

/**
 * Returns an instance of {@link RemoteTransactionManager}.
 *
 * @since 14.0
 */
public class RemoteTransactionManagerLookup implements TransactionManagerLookup {

   private static final RemoteTransactionManagerLookup INSTANCE = new RemoteTransactionManagerLookup();

   private RemoteTransactionManagerLookup() {
   }

   public static TransactionManagerLookup getInstance() {
      return INSTANCE;
   }

   @Override
   public TransactionManager getTransactionManager() {
      return RemoteTransactionManager.getInstance();
   }

   @Override
   public String toString() {
      return "RemoteTransactionManagerLookup";
   }
}
