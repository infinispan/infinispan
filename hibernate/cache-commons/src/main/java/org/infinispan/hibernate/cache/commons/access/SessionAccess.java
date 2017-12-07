package org.infinispan.hibernate.cache.commons.access;

import org.hibernate.engine.transaction.spi.IsolationDelegate;

import javax.transaction.Synchronization;
import java.util.ServiceLoader;

public interface SessionAccess {

   TransactionCoordinatorAccess getTransactionCoordinator(Object session);

   long getTimestamp(Object session);

   static SessionAccess findSessionAccess() {
      ServiceLoader<SessionAccess> loader = ServiceLoader.load(SessionAccess.class);
      return loader.iterator().next();
   }

   interface TransactionCoordinatorAccess {

      void registerLocalSynchronization(Synchronization sync);

      IsolationDelegate createIsolationDelegate();

      boolean isJoined();

   }

}
