package org.infinispan.hibernate.cache.commons.access;

import javax.transaction.Synchronization;

final class SessionThreadLocal {

   private final ThreadLocal<Object> currentSession = new ThreadLocal<>();

   // TODO: Make static
   private final SessionAccess sessionAccess;

   public SessionThreadLocal() {
      this.sessionAccess = SessionAccess.findSessionAccess();
   }

   public void setSession(Object session) {
      currentSession.set(session);
   }

   public void remove() {
      currentSession.remove();
   }

   Object getSession() {
      return currentSession.get();
   }

   public boolean hasTransactionCoordinator() {
      Object session = currentSession.get();
      return sessionAccess.getTransactionCoordinator(session) != null;
   }

   public void registerSynchronization(Synchronization sync) {
      Object session = currentSession.get();
      sessionAccess.getTransactionCoordinator(session).registerLocalSynchronization(sync);
   }

}
