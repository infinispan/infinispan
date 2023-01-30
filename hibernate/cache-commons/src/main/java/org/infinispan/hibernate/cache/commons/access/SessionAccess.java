package org.infinispan.hibernate.cache.commons.access;

import java.util.ServiceLoader;

import org.hibernate.jdbc.WorkExecutorVisitable;

import jakarta.transaction.Synchronization;

public interface SessionAccess {

   TransactionCoordinatorAccess getTransactionCoordinator(Object session);

   long getTimestamp(Object session);

   static SessionAccess findSessionAccess() {
      ServiceLoader<SessionAccess> loader = ServiceLoader.load(SessionAccess.class, SessionAccess.class.getClassLoader());
      return loader.iterator().next();
   }

   interface TransactionCoordinatorAccess {

      void registerLocalSynchronization(Synchronization sync);

      void delegateWork(WorkExecutorVisitable<Void> workExecutorVisitable, boolean requiresTransaction);

      boolean isJoined();

   }

}
