package org.infinispan.hibernate.cache.commons.access;

import java.util.ServiceLoader;

import org.hibernate.engine.transaction.spi.IsolationDelegate;

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

      IsolationDelegate createIsolationDelegate();

      boolean isJoined();

   }

}
