package org.infinispan.query.backend;

import javax.transaction.Synchronization;

import org.hibernate.search.backend.TransactionContext;

final class NoTransactionContext implements TransactionContext {

   public static final NoTransactionContext INSTANCE = new NoTransactionContext();

   private NoTransactionContext() {
   }

   @Override
   public boolean isTransactionInProgress() {
      return false;
   }

   @Override
   public Object getTransactionIdentifier() {
      throw new UnsupportedOperationException();
   }

   @Override
   public void registerSynchronization(Synchronization synchronization) {
      throw new UnsupportedOperationException();
   }
}
