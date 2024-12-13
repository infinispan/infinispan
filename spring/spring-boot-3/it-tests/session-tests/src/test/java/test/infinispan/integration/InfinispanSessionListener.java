package test.infinispan.integration;

import jakarta.servlet.http.HttpSession;
import jakarta.servlet.http.HttpSessionEvent;
import jakarta.servlet.http.HttpSessionListener;

public class InfinispanSessionListener implements HttpSessionListener {

   private HttpSession createdSession;
   private HttpSession destroyedSession;

   @Override
   public void sessionCreated(HttpSessionEvent httpSessionEvent) {
      createdSession = httpSessionEvent.getSession();
   }

   @Override
   public void sessionDestroyed(HttpSessionEvent httpSessionEvent) {
      destroyedSession = httpSessionEvent.getSession();
   }

   public HttpSession getCreatedSession() {
      return createdSession;
   }

   public HttpSession getDestroyedSession() {
      return destroyedSession;
   }
}
