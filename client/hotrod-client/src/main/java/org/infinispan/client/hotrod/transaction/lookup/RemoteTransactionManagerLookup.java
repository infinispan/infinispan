package org.infinispan.client.hotrod.transaction.lookup;

import javax.transaction.TransactionManager;

import org.infinispan.client.hotrod.transaction.manager.RemoteTransactionManager;
import org.infinispan.commons.tx.lookup.TransactionManagerLookup;

/**
 * Returns an instance of {@link RemoteTransactionManager}.
 *
 * @author Pedro Ruivo
 * @since 9.3
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
