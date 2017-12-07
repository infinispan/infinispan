package org.infinispan.hibernate.cache.main.access;

import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.engine.transaction.spi.IsolationDelegate;
import org.hibernate.resource.transaction.spi.TransactionCoordinator;
import org.infinispan.hibernate.cache.commons.access.SessionAccess;

import javax.transaction.Synchronization;

public final class SessionAccessImpl implements SessionAccess {

   @Override
   public TransactionCoordinatorAccess getTransactionCoordinator(Object session) {
      return session == null ? null
         : new TransactionCoordinatorAccessImpl(unwrap(session).getTransactionCoordinator());
   }

   @Override
   public long getTimestamp(Object session) {
      return unwrap(session).getTimestamp();
   }

   private static SharedSessionContractImplementor unwrap(Object session) {
      return (SharedSessionContractImplementor) session;
   }

   private static final class TransactionCoordinatorAccessImpl implements TransactionCoordinatorAccess {

      private final TransactionCoordinator txCoordinator;

      public TransactionCoordinatorAccessImpl(TransactionCoordinator txCoordinator) {
         this.txCoordinator = txCoordinator;
      }

      @Override
      public void registerLocalSynchronization(Synchronization sync) {
         txCoordinator.getLocalSynchronizations().registerSynchronization(sync);
      }

      @Override
      public IsolationDelegate createIsolationDelegate() {
         return txCoordinator.createIsolationDelegate();
      }

      @Override
      public boolean isJoined() {
         return txCoordinator.isJoined();
      }

   }

}
